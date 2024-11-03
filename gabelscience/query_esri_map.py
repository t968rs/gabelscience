import os
from concurrent.futures.process import ProcessPoolExecutor
import threading
from tqdm import tqdm
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import geopandas as gpd
import typing as T
import logging
import time
import references.online_links
from references.online_links import NFHL_IDS, EFFECTIVE_NFHL_URL, INFO_SUFFIX, NFHL_LAYER_NAMES
from references.fema_tables import UNIQUE_IDS
import pandas as pd
import yaml
import shutil
from datetime import datetime
from src.d00_utils.open_spatial import open_fc_any
from src.d00_utils.system import increment_file_naming

log_path = f"./logs/{__name__}.log"
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename=log_path, filemode="a",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


UNWANTED_FIELDS = ['SHAPE.STArea()', 'SHAPE.STLength()', 'GFID', 'GlobalID']

TRUNCATED_PBAR = '{desc}: {percentage:3.0f}%| {n_fmt}/{total_fmt} [{postfix}]'

FOLDER_NAMES = {
    "00": "downloaded", "01": "attrbutes", "02": "sorted", "03": "shp",
    "__STATEWIDE": "statewide"}


def append_data_to_file(data, file_path):
    existing_data = gpd.read_file(file_path)
    new_data = gpd.GeoDataFrame.from_features(data)
    combined_data = gpd.pd.concat([existing_data, new_data], ignore_index=True)
    combined_data.to_file(file_path)
    return file_path


def request(url, timeout=None):
    if timeout is None:
        timeout = 60

    req = requests.get(url)

    if req.status_code != 200:
        stime = time.time()
        while req.status_code != 200 or time.time() - stime < timeout:
            req = requests.get(url)
            if req.status_code == 200:
                break
            if time.time() - stime > timeout:
                logger.error(f"Request failed with status code {req.status_code}")
                logger.error(f"URL: {url}")
                return url

    return req.json()

def audit_field(path, field):
    gdf = open_fc_any(path)
    valueslist = gdf[field].tolist()
    gdf = None
    return valueslist


def features_to_disk(features, outpath, field=None):
    gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4269')
    count = len(gdf)

    if field is not None:
        valuelist = gdf[field].tolist()
    else:
        valuelist = None

    gdf.to_file(outpath, driver='GeoJSON')
    gdf = None
    return outpath, count, valuelist


def get_features(geojson):
    return [f for f in geojson['features']]


def get_state_fips_df():
    state_fips_url = references.online_links.STATE_FIPS
    state_fips_df = gpd.pd.read_csv(state_fips_url, sep="|")
    state_fips_df.columns = [c.strip() for c in state_fips_df.columns]
    state_fips_df["STATE"] = state_fips_df["STATE"].apply(lambda x: f"{x:02d}")
    state_fips_df['STATENS'] = state_fips_df['STATENS'].apply(lambda x: f"{x:011d}")
    state_fips_df['STATE_NAME'] = state_fips_df['STATE_NAME'].apply(lambda x: x.strip())
    state_fips_df['STUSAB'] = state_fips_df['STUSAB'].apply(lambda x: x.strip())
    return state_fips_df


class Download_NFHL:

    def __init__(self, url, out_loc, lyr_no):
        self.url = url
        self.out_loc = out_loc
        self.date_updated, self.extra_info = self.get_date_updated()

        self.out_dirs = {k: f"{out_loc}/{lyr_no}_{v}" for k, v in FOLDER_NAMES.items()}

        if lyr_no not in NFHL_IDS:
            raise ValueError(f"Layer Number {lyr_no} not in NFHL_IDS")
        self.lyr_no = lyr_no

        self.extant_ids = []
        self.new_ids = []
        self.state_aggs = {}

    def add_state_columns(self, file):
        # Save the GeoDataFrame to a new shapefile
        inname = os.path.basename(file)
        name, ext = os.path.splitext(inname)
        outpath = f"{self.out_dirs.get("01")}/{name}{ext}"

        if not os.path.exists(outpath):

            # Add state columns to the GeoDataFrame
            in_gdf = open_fc_any(file)
            in_gdf['STATE'] = in_gdf['DFIRM_ID'].apply(lambda x: f"{x[:2]}")


            in_gdf.to_file(outpath)

        in_gdf = None
        return outpath

    def concat_to_state(self, fips, input_file):
        statelock, inputlock = threading.Lock(), threading.Lock()
        state_file = self.state_aggs.get(fips)
        with statelock, inputlock:
            state_gdf = open_fc_any(state_file)
            b_gdf = open_fc_any(input_file)
            to_add = b_gdf.loc[b_gdf['STATE'] == fips]
            state_gdf = gpd.pd.concat([state_gdf, to_add], ignore_index=True)
            state_gdf.to_file(state_file)
            state_gdf, b_gdf = None, None
            statelock.release(), inputlock.release()
        return input_file

    def get_date_updated(self):
        url = self.url + "/" + INFO_SUFFIX
        item_info = requests.get(url).json()
        date_str = item_info['snippet']

        # Set up saving situation
        yaml_folder = f"{self.out_loc}/yaml"
        os.makedirs(yaml_folder, exist_ok=True)
        outfile = f"{yaml_folder}/date_updated.yaml"

        today = datetime.today().strftime('%Y%m%d')
        print(f"Today: {today}")

        # Write it
        parts = date_str.split(" ")
        int_s = {p: int(p) for p in parts if p.isdigit()}
        for p, no in int_s.items():
            if 10 > len(p) >=8:
                with open(outfile, "w") as f:
                    yaml.dump_all([int_s], f)
                    yaml.dump_all([item_info], f)
                outfile_2 = f"{yaml_folder}/date_updated_{today}.yaml"
                shutil.copy2(outfile, outfile_2)
                return p, item_info

    def get_offset(self):
        folder = self.out_dirs.get("00")
        saved = [f"{folder}/{f}" for f in os.listdir(folder) if f.endswith('.geojson')]

        total_features = 0
        pbar = tqdm(total=len(saved), desc="Getting Offset")

        futures = []
        with ProcessPoolExecutor(int(os.cpu_count() * 0.85)) as ex:
            for file in saved:
                futures.append(
                    ex.submit(open_fc_any, fc_path=file, count_only=True))
            for future in as_completed(futures):
                total_features += future.result()
                pbar.update(1)
                pbar.set_postfix({"Total Features": total_features})

        pbar.close(), ex.shutdown()
        return total_features, len(saved)

    def _get_extant_ids(self, field='OBJECTID'):
        folder = self.out_dirs.get("00")
        saved = [f"{folder}/{f}" for f in os.listdir(folder) if f.endswith('.geojson')]

        id_list = []
        pbar = tqdm(total=len(saved), desc=f"Getting IDs ({field})")

        futures = []
        with ProcessPoolExecutor(int(os.cpu_count() * 0.85)) as ex:
            for file in saved:
                futures.append(
                    ex.submit(audit_field, file, field=field))
            for future in as_completed(futures):
                id_list.append(future.result())
                pbar.update(1)
                pbar.set_postfix({"Total Features": len(id_list)})

        pbar.close(), ex.shutdown()
        return id_list, len(saved)


    def query_map_service_offsets(self, service_url,
                          fields: T.Union[str, list]='*', crs=4269, offset_start=0, extant_files=None):
        where = '1=1'
        local_folder = self.out_dirs.get("00")

        if extant_files is None:
            extant_files = []
        lyr_no = service_url.split('/')[-1]
        local_folder = f"{local_folder}/{lyr_no}_00"
        os.makedirs(local_folder, exist_ok=True)

        # Define the query endpoint
        query_url = f"{service_url}/query"

        # Determine the total number of records for the given where clause
        count_params = {
            "where": where,
            "returnCountOnly": True,
            "f": "json"
        }
        tot_records_req = requests.get(query_url, params=count_params)
        print(tot_records_req.url)
        tot_records_json = tot_records_req.json()
        tot_records = int(tot_records_json["count"])
        print(f"Total Records: {tot_records}")

        # Determine the step size for pages
        step_json = requests.get(service_url, params={'f': 'json'}).json()
        step = min(step_json["maxRecordCount"] - 1, 100)
        no_steps = tot_records // step
        print(f'\tStep Size: {step}, Number of Steps: {no_steps}')

        # Query to get Fields
        if fields == '*':
            fields_json = requests.get(f"{service_url}/", params={'f': 'json'}).json()
            fields = [f['name'] for f in fields_json['fields'] if f['name'] not in UNWANTED_FIELDS]
        print(f"Fields: {fields}")


        # Define query parameters to get records
        query_params = {
            "where": where,
            "outFields": ', '.join(fields) if isinstance(fields, list) else fields,
            "outSr": crs,
            "f": "geojson",
            "orderByFields": "OBJECTID",
            "returnGeometry": True,
            "resultRecordCount": step
        }

        # Loop through each page of query results concurrently
        worker_threads = 100
        failed_urls, disk_futures = [], []
        no_feat_saved, no_files_saved = offset_start, extant_files
        with ThreadPoolExecutor(worker_threads) as ex, ProcessPoolExecutor(int(os.cpu_count() * 0.7)) as process_ex:
            pbar_req = tqdm(total=2 * no_steps, desc=f"Qry Srvc"
                                                     f"", position=0)
            pbar_req.update(offset_start)
            pbar_batches = tqdm(total=no_steps, desc="Writing Batched responses", position=1)
            pbar_feats = tqdm(total=tot_records, desc="Tracking Features Saved", position=2)
            pbar_feats.update(no_feat_saved)

            req_futures = []
            for offset in range(offset_start, tot_records, step):
                # Create each offset query
                query_params['resultOffset'] = offset
                offset_query = urllib.parse.urlencode(query_params)
                offset_query_url = f"{query_url}?{offset_query}"
                req_futures.append(ex.submit(request, offset_query_url, timeout=60))

            # Compile all futures once they're complete
            batch_size = worker_threads
            current_batch = []  # List to store GeoJSON features temporarily
            fut_recd, fut_prcd = 0, 0
            total_futures = len(req_futures)

            # Use `as_completed` to process futures as they finish
            for future in as_completed(req_futures):
                fut_recd += 1
                pbar_req.set_postfix({"Rcd": fut_recd, "Prc": fut_prcd, "Tot": total_futures})
                result = future.result()
                pbar_req.update(1)
                if 'features' in result:
                    current_batch.append(result)  # Add the result to the current batch
                else:
                    failed_urls.append(result)  # Collect failed requests

                # If the batch size is reached, save to disk and clear the batch
                if len(current_batch) >= batch_size:
                    # Concatenate the GeoJSON features into a GeoDataFrame
                    geos = [g for g in current_batch if 'features' in g]
                    if geos:
                        for result in process_ex.map(get_features, geos):
                            # Save the GeoDataFrame to disk
                            no_features = len(result)
                            suffix = no_features + no_feat_saved
                            outpath = f"{local_folder}/batch_{suffix}.geojson"
                            increment_file_naming(outpath)
                            no_feat_saved += no_features
                            disk_futures.append(process_ex.submit(features_to_disk, result, outpath))

                    # Clear the batch from memory
                    pbar_batches.update(1)
                    pbar_batches.set_postfix({"Total Saved": no_feat_saved})
                    current_batch.clear()

                pbar_req.update(1)
                fut_prcd += 1

            # Handle any remaining data in `current_batch` after the loop
            if current_batch:
                geos = [g for g in current_batch if 'features' in g]
                if geos:
                    for result in process_ex.map(get_features, geos):
                        # Save the GeoDataFrame to disk
                        no_features = len(result)
                        suffix = no_features * no_feat_saved
                        outpath = f"{local_folder}/batch_{suffix}.geojson"
                        increment_file_naming(outpath)
                        no_files_saved += 1
                        disk_futures.append(process_ex.submit(features_to_disk, result, outpath))

                        # Clear the batch from memory
                        pbar_batches.update(1)
                        pbar_batches.set_postfix({"Total Batches": no_feat_saved})
                        current_batch.clear()

                    pbar_req.update(1)
                    fut_prcd += 1

            # Wait for all the saved futures to complete
            outpaths = []
            new_count_saved = 0
            for f in as_completed(disk_futures):
                outpath, count = f.result()
                pbar_feats.update(count)
                pbar_feats.set_postfix({"New Saved": new_count_saved, "Files Saved": no_files_saved,
                                        "Total Saved": no_feat_saved})
                outpaths.append(outpath)
                no_files_saved += 1
                new_count_saved += count

        return outpaths, no_feat_saved, failed_urls

    def query_map_service_ids(self, service_url,
                              fields: T.Union[str, list]='*',
                              crs=4269,
                              id_field='OBJECTID',
                              start_file_count=0,):

        formatted_values = [str(v) for v in self.extant_ids]
        where = f"{id_field} NOT IN ({formatted_values})"
        local_folder = self.out_dirs.get("00")

        # Define the query endpoint
        query_url = f"{service_url}/query"

        # Determine the total number of records for the given where clause
        count_params = {
            "where": where,
            "returnCountOnly": True,
            "f": "json"
        }
        tot_records_req = requests.get(query_url, params=count_params)
        print(tot_records_req.url)
        tot_records_json = tot_records_req.json()
        tot_records = int(tot_records_json["count"])
        print(f"Total Records: {tot_records}")

        # Determine the step size for pages
        step_json = requests.get(service_url, params={'f': 'json'}).json()
        step = min(step_json["maxRecordCount"] - 1, 100)
        no_steps = tot_records // step
        print(f'\tStep Size: {step}, Number of Steps: {no_steps}')

        # Query to get Fields
        if fields == '*':
            fields_json = requests.get(f"{service_url}/", params={'f': 'json'}).json()
            fields = [f['name'] for f in fields_json['fields'] if f['name'] not in UNWANTED_FIELDS]
        print(f"Fields: {fields}")


        # Define query parameters to get records
        query_params = {
            "where": where,
            "outFields": ', '.join(fields) if isinstance(fields, list) else fields,
            "outSr": crs,
            "f": "geojson",
            "orderByFields": id_field,
            "returnGeometry": True,
            "resultRecordCount": step
        }

        # Loop through each page of query results concurrently
        worker_threads = 100
        failed_urls, disk_futures = [], []
        no_feat_saved, no_files_saved = len(self.extant_ids), start_file_count
        with ThreadPoolExecutor(worker_threads) as ex, ProcessPoolExecutor(int(os.cpu_count() * 0.7)) as process_ex:
            pbar_req = tqdm(total=2 * no_steps, desc=f"Qry Srvc"
                                                     f"", position=0)
            pbar_req.update(len(self.extant_ids))
            pbar_batches = tqdm(total=no_steps, desc="Writing Batched responses", position=1)
            pbar_feats = tqdm(total=tot_records, desc="Tracking Features Saved", position=2)
            pbar_feats.update(no_feat_saved)

            req_futures = []
            for offset in range(0, tot_records - len(self.extant_ids), step):
                # Create each offset query
                query_params['resultOffset'] = offset
                offset_query = urllib.parse.urlencode(query_params)
                offset_query_url = f"{query_url}?{offset_query}"
                req_futures.append(ex.submit(request, offset_query_url, timeout=60))

            # Compile all futures once they're complete
            batch_size = worker_threads
            current_batch = []  # List to store GeoJSON features temporarily
            fut_recd, fut_prcd = 0, 0
            total_futures = len(req_futures)

            # Use `as_completed` to process futures as they finish
            for future in as_completed(req_futures):
                fut_recd += 1
                pbar_req.set_postfix({"Rcd": fut_recd, "Prc": fut_prcd, "Tot": total_futures})
                result = future.result()
                pbar_req.update(1)

                # If the result is a GeoJSON feature collection, add it to the current batch
                if 'features' in result:
                    current_batch.append(result)  # Add the result to the current batch
                else:
                    failed_urls.append(result)  # Collect failed requests

                # If the batch size is reached, save to disk and clear the batch
                if len(current_batch) >= batch_size:
                    # Concatenate the GeoJSON features into a GeoDataFrame
                    geos = [g for g in current_batch if 'features' in g]
                    if geos:
                        for result in process_ex.map(get_features, geos):
                            # Save the GeoDataFrame to disk
                            no_features = len(result)
                            suffix = no_features + no_feat_saved
                            outpath = f"{local_folder}/batch_{suffix}.geojson"
                            increment_file_naming(outpath)
                            no_feat_saved += no_features
                            disk_futures.append(
                                process_ex.submit(
                                    features_to_disk, result, outpath))

                    # Clear the batch from memory
                    pbar_batches.update(1)
                    pbar_batches.set_postfix({"Total Saved": no_feat_saved})
                    current_batch.clear()

                pbar_req.update(1)
                fut_prcd += 1

            # Handle any remaining data in `current_batch` after the loop
            if current_batch:
                geos = [g for g in current_batch if 'features' in g]
                if geos:
                    for result in process_ex.map(get_features, geos):
                        # Save the GeoDataFrame to disk
                        no_features = len(result)
                        suffix = no_features * no_feat_saved
                        outpath = f"{local_folder}/batch_{suffix}.geojson"
                        increment_file_naming(outpath)
                        no_files_saved += 1
                        disk_futures.append(process_ex.submit(features_to_disk, result, outpath))

                        # Clear the batch from memory
                        pbar_batches.update(1)
                        pbar_batches.set_postfix({"Total Batches": no_feat_saved})
                        current_batch.clear()

                    pbar_req.update(1)
                    fut_prcd += 1

            # Wait for all the saved futures to complete
            outpaths = []
            new_count_saved = 0
            for f in as_completed(disk_futures):
                outpath, count, ids_downloaded = f.result()
                pbar_feats.update(count)
                pbar_feats.set_postfix({"New Saved": new_count_saved, "Files Saved": no_files_saved,
                                        "Total Saved": no_feat_saved})
                self.new_ids.extend(ids_downloaded)
                outpaths.append(outpath)
                no_files_saved += 1
                new_count_saved += count

        return failed_urls

    def create_state_geojsons(self, sub_lyr='28'):
        # Get/prep state FIPS df
        state_fips_df = get_state_fips_df()
        print(f'\nState FIPS: \n{state_fips_df.head()}')
        states = state_fips_df["STATE"].unique().tolist()

        # Get column list
        # Query to get Fields
        fields_json = requests.get(f"{service_url}/", params={'f': 'json'}).json()
        fields = [f['name'] for f in fields_json['fields'] if f['name'] not in UNWANTED_FIELDS]
        print(f"Fields: {fields}")
        col_list = fields + state_fips_df.columns.tolist()
        slice_size = 10
        for i in range(0, len(col_list), slice_size):
            print(col_list[i:i + slice_size])

        state_files = {}
        pbar = tqdm(total=len(states), desc="Statewide Files")
        print(f"Creating Statewide Files")
        print(f"States: {states}")
        for state in states:
            statename = state_fips_df.loc[state_fips_df["STATE"] == state, "STATE_NAME"].values[0]
            statepath = f"{self}/S_{sub_lyr}_{state}.geojson"
            exists = os.path.exists(statepath)
            pbar.set_postfix({"State": statename, "Exists": exists})
            if not exists:
                state_gdf = gpd.GeoDataFrame(columns=col_list, crs=gdf1.crs, geometry=gdf1.geometry)
                state_gdf.to_file(f"{self.out_dirs.get("__STATEWIDE")}/S_{sub_lyr}_{state}.geojson")
            state_files[state] = statepath
            pbar.update(1)
        pbar.close()

    def org_by_state(self, file_list, sub_lyr='28'):
        # Define the folder to save the state-added shapefiles
        unique_parents = list(set([os.path.dirname(f) for f in file_list]))
        if not len(unique_parents) == 1:
            raise ValueError("All files must be in the same parent folder")
        parent_folder = unique_parents[0]
        pre_index_str = parent_folder.split('_')[-1]
        this_index = "0" + str(int(pre_index_str) + 1) if int(pre_index_str) < 9 else str(int(pre_index_str) + 1)
        state_added_folder = f"{parent_folder}/{sub_lyr}_{this_index}"
        print(f"State Added Folder: {state_added_folder}")
        print(f'Parent Folder: {parent_folder}')
        statewide_folder = f"{parent_folder}/{sub_lyr}__STATEWIDE"
        os.makedirs(state_added_folder, exist_ok=True)
        os.makedirs(statewide_folder, exist_ok=True)



        # Get all the GEOJSON files
        print(f'Starting Column Additions')
        state_path_futs, out_paths = [], []
        with ProcessPoolExecutor(int(os.cpu_count() * 0.70)) as pex:  # ProcessPoolExecutor(int(os.cpu_count() * 0.85)) as proc_ex:
            for path in file_list:
                state_path_futs.append(
                    pex.submit(self.add_state_columns, path))

            # Add states columns to each GeoDataFrame
            pbar = tqdm(total=1 * len(file_list), desc="Adding State Column")
            for f in as_completed(state_path_futs):
                pbar.update(1)
                path_with_states = f.result()
                out_paths.append(path_with_states)
        pex.shutdown()

        return out_paths, state_files

    def sort_concat_to_state(self):

        with ProcessPoolExecutor(int(os.cpu_count() * 0.70)) as pex:
            # Concatenate the state-added GEOJSON to the statewide GEOJSON
            pbar2 = tqdm(total=2 * len(self.state_aggs) * len(out_paths), desc="Sort + Concat")
            for state, statepath in state_files.items():
                for path in out_paths:
                    out_futs.append(
                        pex.submit(concat_to_state,state, statepath, path))
                    pbar2.update(1)

            # Wait for the state-added GEOJSON to be concatenated to the statewide GEOJSON
            for future in as_completed(out_futs):
                finished.append(future.result())
                pbar2.update(1)
        pbar2.close()
        pex.shutdown()

        for state, state_file in state_files.items():
            if state_file not in finished:
                print(f"State {state} not in finished list")
        print(f"Finished: {finished}")


    def get_features_from_service(self):

        # Query the effective NFHL data
        fcname = NFHL_LAYER_NAMES.get(self.lyr_no)
        id_field = UNIQUE_IDS.get(fcname.lower())
        self.extant_ids, extant_count = self._get_extant_ids(id_field)

        failed_urls = self.query_map_service_ids(self.url, fields='*', crs=4269, start_file_count=extant_count)

        # Save failed URLs to a file
        failed_yaml = f"{self.out_loc}/yaml/failed_urls.yaml"
        with open(failed_yaml, "w") as f:
            yaml.dump(failed_urls, f)

        # Save all IDs to a file
        all_ids_yaml = f"{self.out_loc}/yaml/all_ids_{self.lyr_no}.yaml"
        with open(all_ids_yaml, "w") as f:
            yaml.dump(self.new_ids, f)
            yaml.dump(self.extant_ids, f)




if __name__ == "__main__":
    base_url = EFFECTIVE_NFHL_URL
    out_loc = "../test_data/downloads/nfhl"
    # date_updated, extra_info = get_date_updated(out_loc + "/yaml")

    # Query the effective NFHL data
    # starting_offset, files = get_offset("../test_data/downloads/nfhl/28_00")
    # print(f"Starting Offset: {starting_offset}")
    # outyaml_loc = r"D:\o\OneDrive - kevin gabelman\nfhl\yaml"
    # start_yaml = os.path.join(outyaml_loc, "starting_specs.yaml")
    # with open(start_yaml, "w") as f:
    #     yaml.dump({"offset": starting_offset, "files": files}, f)

    # outputs, count_features, failed_url_list = query_map_service(effective_nfhl_url,
    #                                                              local_folder=out_loc,
    #                                                              offset_start=starting_offset,
    #                                                              extant_files=files)
    # logger.info(f"Failed URLs: {failed_url_list}")
    #
    extant_batches = [f"{out_loc}/28_00/{f}" for f in os.listdir(f"{out_loc}/28_00") if f.endswith('.geojson')]
    outputs = [f for f in extant_batches if 'batch' in f]
    _, outpaths = org_by_state(outputs, sub_lyr='28')
