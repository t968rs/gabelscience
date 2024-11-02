import os
from concurrent.futures.process import ProcessPoolExecutor

from tqdm import tqdm
import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import geopandas as gpd
import typing as T
import logging
import time
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



def append_data_to_file(data, file_path):
    existing_data = gpd.read_file(file_path)
    new_data = gpd.GeoDataFrame.from_features(data)
    combined_data = gpd.pd.concat([existing_data, new_data], ignore_index=True)
    combined_data.to_file(file_path)
    return file_path

def concat_to_state(input_file, states_files_lookup):
    gdf = open_fc_any(input_file)
    for state, state_file in states_files_lookup.items():
        state_gdf = open_fc_any(state_file)
        to_add = gdf.loc[gdf['STATE'] == state]
        state_gdf = gpd.pd.concat([state_gdf, to_add], ignore_index=True)
        state_gdf.to_file(state_file)
        state_gdf.close()
    gdf.close()
    return input_file

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

def features_to_disk(features, outpath):
    gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4269')
    count = len(gdf)
    gdf.to_file(outpath, driver='GeoJSON')
    return outpath, count

def get_features(geojson):
    return [f for f in geojson['features']]

def get_state_fips_df():
    state_fips_url = "https://www2.census.gov/geo/docs/reference/state.txt"
    state_fips_df = gpd.pd.read_csv(state_fips_url, sep="|")
    state_fips_df.columns = [c.strip() for c in state_fips_df.columns]
    state_fips_df["STATE"] = state_fips_df["STATE"].apply(lambda x: f"{x:02d}")
    state_fips_df['STATENS'] = state_fips_df['STATENS'].apply(lambda x: f"{x:011d}")
    state_fips_df['STATE_NAME'] = state_fips_df['STATE_NAME'].apply(lambda x: x.strip())
    state_fips_df['STUSAB'] = state_fips_df['STUSAB'].apply(lambda x: x.strip())
    return state_fips_df

def get_date_updated(yaml_folder):
    url = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/info/iteminfo?f=pjson"
    item_info = requests.get(url).json()
    date_str = item_info['snippet']

    # Set up saving situation
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

def get_offset(folder):
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

def query_map_service(service_url, local_folder, where='1=1',
                      fields: T.Union[str, list]='*', crs=4269, offset_start=0, extant_files=None):
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

def org_by_state(file_list, sub_lyr='28'):
    # Define the folder to save the state-added shapefiles
    unique_parents = list(set([os.path.dirname(f) for f in file_list]))
    if not len(unique_parents) == 1:
        raise ValueError("All files must be in the same parent folder")
    parent_folder = unique_parents[0]
    pre_index_str = parent_folder.split('_')[-1]
    this_index = "0" + str(int(pre_index_str) + 1) if int(pre_index_str) < 9 else str(int(pre_index_str) + 1)
    state_added_folder = f"{parent_folder}/{sub_lyr}_{this_index}"
    statewide_folder = f"{parent_folder}/{sub_lyr}__STATEWIDE"
    os.makedirs(state_added_folder, exist_ok=True)
    os.makedirs(statewide_folder, exist_ok=True)

    def add_state_columns(file):
        # Add state columns to the GeoDataFrame
        in_gdf = open_fc_any(file)
        in_gdf['STATE'] = in_gdf['DFIRM_ID'].apply(lambda x: f"{x[:2]}")

        # Save the GeoDataFrame to a new shapefile
        inname = os.path.basename(file)
        name, ext = os.path.splitext(inname)
        outpath = f"{state_added_folder}/{name}{ext}"
        in_gdf.to_file(outpath)
        return outpath

    # Get/prep state FIPS df
    state_fips_df = get_state_fips_df()
    print(f'\nState FIPS: \n{state_fips_df.head()}')
    states = state_fips_df["STATE"].unique().tolist()

    # Get column list
    gdf1 = open_fc_any(file_list[0])
    col_list = gdf1.columns.tolist() + state_fips_df.columns.tolist()
    state_files = {}
    for state in states:
        state_gdf = gpd.GeoDataFrame(columns=col_list, crs=gdf1.crs, geometry=gdf1.geometry)
        state_gdf.to_file(f"{statewide_folder}/S_{sub_lyr}_{state}.geojson")
        state_files[state] = f"{statewide_folder}/S_{sub_lyr}_{state}.geojson"

    # Get all the GEOJSON files
    state_path_futs, out_paths = [], []
    with ThreadPoolExecutor(30) as ex:
        for path in file_list:
            state_path_futs.append(ex.submit(add_state_columns, path))

        # Add states columns to each GeoDataFrame
        pbar = tqdm(total=1 * len(file_list), desc="Adding State Column")
        for f in as_completed(state_path_futs):
            pbar.update(1)
            out_paths.append(f.result())
        pbar.close()

        for path in out_paths:
            new_state_files = {k: v for k, v in state_files.items()}
            state_path_futs.append(
                ex.submit(concat_to_state,path, new_state_files))

        finished = []
        for f in as_completed(state_path_futs):
            finished.append(f.result())

        for state, state_file in state_files.items():
            if state_file not in finished:
                print(f"State {state} not in finished list")
                print(f"Finished: {finished}")

    return out_paths, state_files


if __name__ == "__main__":
    effective_nfhl_url = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28"
    out_loc = "../test_data/downloads/nfhl"
    # date_updated, extra_info = get_date_updated(out_loc + "/yaml")

    # Query the effective NFHL data
    starting_offset, files = get_offset("../test_data/downloads/nfhl/28_00")
    print(f"Starting Offset: {starting_offset}")
    outputs, count_features, failed_url_list = query_map_service(effective_nfhl_url,
                                                                 local_folder=out_loc,
                                                                 offset_start=starting_offset,
                                                                 extant_files=files)
    logger.info(f"Failed URLs: {failed_url_list}")

    _, outpaths = org_by_state(outputs, sub_lyr='28')
