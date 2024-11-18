import os
from tqdm import tqdm
import requests
import urllib.parse
from concurrent.futures.process import ProcessPoolExecutor as PPE
from concurrent.futures import ThreadPoolExecutor as TPE, as_completed
import geopandas as gpd
import numpy as np
import typing as T
from references.online_links import NFHL_IDS, EFFECTIVE_NFHL_URL, INFO_SUFFIX, NFHL_FC_NAMES, get_state_fips_df
from references.fema_tables import FEMATable
import yaml
import shutil
from datetime import datetime
from src.d00_utils.loggers import getalogger
from src.d00_utils.postgis_helpers import get_list_of_records, logger
from src.d00_utils.gbounds import gBounds
from src.d01_processing.np_helpers import save_list_to_npy, load_from_numpy
from src.d01_processing.spatial_operations import features_to_disk, get_features, audit_field
from src.d00_utils.open_spatial import open_fc_any
from src.d00_utils.system import increment_file_naming, is_file_recent, compare_file_timestamps
from src.d00_utils.timer import TimerLogger
from src.d04_web.req_helpers import request_with_subset, fetch_json, request

clogger = getalogger("query_esri_map_service", level=10)


UNWANTED_FIELDS = ['SHAPE.STArea()', 'SHAPE.STLength()', 'GFID', 'GlobalID']

TRUNCATED_PBAR = '{desc}: {percentage:3.0f}%| {n_fmt}/{total_fmt} [{postfix}]'

STEPS = [0, 1, 2, 3, "offset", "extant_ids", "effective_ids"]

FOLDER_NAMES = {
    "00": "downloaded", "01": "attributes", "l": "lists", "y": "yaml", "p": "parquet", "gpkg": "gpkg"}


def add_geometry_to_query_params(query_params, geom, crs=None):
    if isinstance(geom, gBounds):
        query_params["geometry"] = geom.to_esri_json()
    elif isinstance(geom, tuple):
        if crs is None:
            crs = 4269
        bounds = gBounds.from_bbox(geom, crs)
        query_params["geometry"] = bounds.to_esri_json()
    else:
        raise ValueError(f"Invalid geometry type: {type(geom)}")

    query_params["geometryType"] = "esriGeometryEnvelope"
    query_params["spatialRel"] = "esriSpatialRelIntersects"
    return query_params


class Download_NFHL:

    def __init__(self, url, out_loc, lyr_no, state=None, query_geometry=None):

        self._serv_url = url
        self.where, self.state = None, None
        # Start timer
        os.makedirs(out_loc, exist_ok=True)
        timepath = f"{out_loc}/{__name__}_timer.log"
        self.stopwatch = TimerLogger(timepath)
        self.stopwatch.start()
        self.stopwatch.log("NEW RUN")
        self._state_info = None
        self.query_geometry = query_geometry

        self.state_df, self.states_list = self.state_info


        if not state:
            self.out_loc = out_loc
            self.where = '1=1'
            self.state = None
            self.fips = None
        else:
            self.out_loc = f"{out_loc}/{state}"
            self.state = state
            self.fips = self.state_df.loc[self.state_df["STUSAB"] == state, "STATE"].values[0]
            self.where = f"DFIRM_ID LIKE '{self.fips}%'"

        if self.state is not None:
            self.out_dirs = {k: f"{self.out_loc}/{lyr_no}_{v}" for k, v in FOLDER_NAMES.items()}
        else:
            self.out_dirs = {k: f"{self.out_loc}/ALL_STATES/{lyr_no}_{v}" for k, v in FOLDER_NAMES.items()}
        self.out_dirs['statewide'] = f"{out_loc}/ALL_STATES/{lyr_no}"
        self.out_dirs["unknowns"] = f"{out_loc}/ALL_STATES/unknowns"
        self.out_dirs["state_shp"] = f"{out_loc}/{self.state}/SHP"

        if lyr_no not in NFHL_IDS:
            raise ValueError(f"Layer Number {lyr_no} not in NFHL_IDS")
        self.lyr_no = lyr_no
        self.query_url = f"{url}/{self.lyr_no}/query"
        self.crs = 4269

        self.tot_service_rec = 0
        lyr_name = NFHL_FC_NAMES.get(str(self.lyr_no))
        self.fema_table = FEMATable.from_table_type(lyr_name)
        self.id_field = self.fema_table.unique_id

        clogger.info(f"State: {state}, ID Field: {self.id_field}, Layer ({self.lyr_no}): {NFHL_IDS.get(self.lyr_no)}")
        self.new_features = 0
        self.extant_ids_path = self.out_dirs.get("l") + f"/{self.lyr_no}_extant_{self.id_field}.npy"
        self.service_ids_path = self.out_dirs.get("l") + f"/{self.lyr_no}_all_{self.id_field}.npy"
        self.ids_to_get = self.out_dirs.get("l") + f"/to_get_{self.lyr_no}"
        self.last_run_yaml = self.out_dirs.get("y") + "/last_run_info.yaml"

        self.connect_test = self._test_service_connection()

    @property
    def state_info(self):
        # Get/prep state FIPS df
        if self._state_info is None:
            self.state_df, self.states_list = get_state_fips_df()
            self._state_info = (self.state_df, self.states_list)
        clogger.info(f'\nState FIPS: \n{self.state_df.head()}')
        return self._state_info

    def _test_service_connection(self):
        # clogger.info(f"Testing connection to {self.query_url}")
        req = requests.get(self.query_url, timeout=60)
        if req.status_code == 200:
            return True
        else:
            clogger.info("Request failed with status code:", req.status_code)
            return False

    def _create_folders(self):
        os.makedirs(self.out_loc, exist_ok=True)
        for k, v in self.out_dirs.items():
            os.makedirs(v, exist_ok=True)
        os.makedirs(self.ids_to_get, exist_ok=True)

    def add_state_column(self, inpath):
        # Save the GeoDataFrame to a new shapefile
        inname = os.path.basename(inpath)
        name, ext = os.path.splitext(inname)
        outpath = f"{self.out_dirs.get("01")}/{name}{ext}"

        if not os.path.exists(outpath):

            # Add state columns to the GeoDataFrame
            in_gdf = open_fc_any(inpath)
            if "OBJECTID" in in_gdf.columns:
                in_gdf["OBJECTID"] = in_gdf["OBJECTID"].astype(int)
            in_gdf['STATE'] = in_gdf['DFIRM_ID'].apply(lambda x: f"{x[:2]}")

            in_gdf.to_file(outpath)

        in_gdf = None
        return outpath

    def _get_date_updated(self):
        url = self._serv_url + "/" + INFO_SUFFIX
        item_info = requests.get(url).json()
        date_str = item_info['snippet']

        # Set up saving situation
        yaml_folder = f"{self.out_loc}/yaml"
        os.makedirs(yaml_folder, exist_ok=True)
        outfile = f"{yaml_folder}/date_updated.yaml"

        today = datetime.today().strftime('%Y%m%d')
        clogger.info(f"Today: {today}")

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
                self.date_updated, self.extra_info = p, item_info

    def _create_download_todo(self, step_size):
        # Clear output folder
        if os.path.exists(self.ids_to_get):
            for f in os.listdir(self.ids_to_get):
                os.remove(f"{self.ids_to_get}/{f}")

        # Get needed feature ids
        clogger.info(f'Getting download to-do list...')

        total_feats = load_from_numpy(self.service_ids_path).astype(np.int32)
        extant = np.array([], dtype=np.int32)

        if os.path.exists(self.extant_ids_path):
            clogger.info(f"Extant IDs exist, loading...")
            extant = load_from_numpy(self.extant_ids_path).astype(np.int32)
            clogger.info(f'Extant: {self.extant_ids_path}, Type: {extant.dtype}')

        if len(extant) > 0:
            clogger.info(f'Some features ({len(extant)}) already downloaded, removing from list...')
            to_get = np.setdiff1d(total_feats, extant, assume_unique=False)
        else:
            to_get = total_feats

        # Record counts
        no_feat_saved = len(extant)
        length_to_get = len(to_get)
        clogger.info(f"Total Features: {len(total_feats)}, Extant: {len(extant)}, To Get: {length_to_get}")

        # Check if there are any IDs to get
        if length_to_get == 0:
            clogger.info("All features are already downloaded.")
            return no_feat_saved, length_to_get, []

        futs, subset_files = [], []
        pbar = tqdm(total=length_to_get, desc="Saving todos in subsets...")
        with TPE() as ex:
            # Save subsets to file
            for i in range(0, length_to_get, step_size):
                start = i
                stop = i + step_size
                outpath = self.ids_to_get + f"/toget_{stop}.npy"

                futs.append(ex.submit(save_list_to_npy, to_get[start:stop], outpath))

            for fut in as_completed(futs):
                try:
                    subset_files.append(fut.result())
                    pbar.update(step_size)
                except Exception as e:
                    clogger.info(f"Error saving subset: {e}")
                    continue

        total_y, extant, to_get = None, None, None

        return no_feat_saved, length_to_get, subset_files

    def _get_extant_ids(self):

        # Get all the GEOJSON files
        folder = self.out_dirs.get("00")
        saved = [f"{folder}/{f}" for f in os.listdir(folder) if f.endswith('.geojson')]
        # Check length of saved
        if not saved:
            return 0, 0
        most_recent_save = max(saved, key=os.path.getmtime)

        # Find last modified file from yaml
        # Check if the most recent save is too old, from list
        valid = compare_file_timestamps(self.extant_ids_path, most_recent_save, hr_tol=1)
        if valid:
            id_length = load_from_numpy(self.extant_ids_path, count_only=True)
            return len(saved), id_length

        # ID Field type
        gdf = open_fc_any(saved[0])
        field_type = gdf[self.id_field].dtype
        if field_type == "object":
            field_type = str
        clogger.info(f"{self.id_field} Field Type: {field_type}, Examples: {gdf[self.id_field].unique().tolist()[:5]}")
        gdf = None

        id_set = set()
        pbar = tqdm(total=len(saved), desc=f"Getting Extant IDs ({self.id_field})", colour="yellow")
        futures = []
        with PPE(int(os.cpu_count() * 0.85)) as ex:
            for file in saved:
                futures.append(
                    ex.submit(audit_field, file, field=self.id_field, field_type=field_type))
            for i, future in enumerate(as_completed(futures)):
                id_set.update(future.result())
                # if i < 2:
                #     clogger.info(f"ID Set: \n{id_set}")
                pbar.set_postfix({"Total Features": len(id_set)})
                pbar.update(1)

        pbar.close(), ex.shutdown()

        # Get IDs from SQL
        print(f"Getting IDs from SQL...")
        sql_ids = set(get_list_of_records("nfhl_tr32", self.fema_table, self.id_field))
        clogger.info(f"SQL IDs: {len(sql_ids)}, GJSON IDs: {len(id_set)}")
        id_set = id_set.union(sql_ids)

        # Make unique
        id_arr = np.array(list(id_set))
        # id_arr = np.unique(id_array)
        id_length = len(id_arr)

        # Save ids to file
        _ = save_list_to_npy(id_arr, self.extant_ids_path)

        clogger.info(f"{self.id_field} Type: {id_arr.dtype}")
        id_arr = None

        return len(saved), id_length

    def _query_map_service_for_ids(self) -> T.Tuple[list, int]:

        """
        Query the map service for all the IDs. Class defines ID field.
        """

        if is_file_recent(self.service_ids_path):
            clogger.info(f"Extant IDs file is recent, skipping...")
            id_length = load_from_numpy(self.service_ids_path, count_only=True)
            self.tot_service_rec = id_length
            return [], id_length

        self.tot_service_rec = self.get_total_record_count()
        step = self.get_step_size()
        query_params = self.construct_id_query_params(step)
        clogger.info(f"Total Records: {self.tot_service_rec}, Step Size: {step}")
        clogger.info(f"Query Params: {query_params}")

        failed_urls, feat_ids = self.fetch_ids_concurrently(query_params, step)
        save_list_to_npy(feat_ids, self.service_ids_path)

        return failed_urls, len(feat_ids)

    def get_total_record_count(self):
        count_params = {
            "where": self.where,
            "returnCountOnly": True,
            "f": "json"
        }
        if self.query_geometry:
            count_params = add_geometry_to_query_params(count_params, self.query_geometry, self.crs)

        try:
            response = requests.get(self.query_url, params=count_params)
            response.raise_for_status()
            count_json = response.json()
            return int(count_json.get("count", 0))
        except requests.RequestException as e:
            clogger.info(f"Error fetching total records: {e}")
            return 0

    def get_step_size(self, reduce_factor=4):
        try:
            response = requests.get(f"{self._serv_url}/{self.lyr_no}", params={'f': 'json'})
            response.raise_for_status()
            step_json = response.json()
            return int(step_json.get("maxRecordCount", 1000) / reduce_factor)
        except requests.RequestException as e:
            clogger.info(f"Error fetching step size: {e}")
            return 100  # Default step size

    def construct_id_query_params(self, step):
        base_params = {
            "where": self.where,
            "f": "json",
            "orderByFields": "OBJECTID",
            "resultRecordCount": step
        }
        if self.id_field == "OBJECTID":
            base_params["returnIdsOnly"] = True
        else:
            if isinstance(self.id_field, list):
                base_params["outFields"] = ["OBJECTID"] + self.id_field
            else:
                base_params["outFields"] = ["OBJECTID", self.id_field]
            base_params.update({
                "returnGeometry": False,
                "returnIdsOnly": False,
            })

        if self.query_geometry:
            base_params = add_geometry_to_query_params(base_params, self.query_geometry, self.crs)

        return base_params

    def construct_features_query_params(self, step, fields=None):
        if not fields:
            fields = self.fetch_fields()
        base_params = {
            "where": self.where,
            "outFields": ', '.join(fields) if isinstance(fields, list) else fields,
            "f": "geojson",
            "orderByFields": "OBJECTID",
            "returnGeometry": True,
            "resultRecordCount": step
        }

        if self.query_geometry:
            base_params = add_geometry_to_query_params(base_params, self.query_geometry, self.crs)

        return base_params

    def construct_rows_query_params(self, step, fields=None):
        if not fields:
            fields = self.fetch_fields()
        base_params = {
            "where": self.where,
            "outFields": ', '.join(fields) if isinstance(fields, list) else fields,
            "f": "json",
            "orderByFields": "OBJECTID",
            "returnGeometry": False,
            "resultRecordCount": step
        }

        if self.query_geometry:
            base_params = add_geometry_to_query_params(base_params, self.query_geometry, self.crs)

        return base_params

    def fetch_fields(self):
        # Query to get Fields
        query_url = f"{self._serv_url}/{self.lyr_no}?f=pjson"
        clogger.info(f"Fields Query URL: {query_url}")

        response = requests.get(query_url, timeout=60)
        clogger.info(f"Response Status Code: {response.status_code}")
        # clogger.info(f"Response Content: {response.text}")

        if response.status_code == 200:
            try:
                fields_json = response.json()
            except requests.exceptions.JSONDecodeError as e:
                clogger.error(f"JSONDecodeError: {e}")
                raise
        else:
            clogger.error(f"Failed to fetch fields. Status Code: {response.status_code}")
            response.raise_for_status()
            clogger.error(f"Failed to fetch fields. Status Code: {response.status_code}")
            clogger.error(f"Failed URL: {response.url}")
            raise ValueError(f"Failed to fetch fields. Status Code: {response.status_code}")

        layer_info_yaml = f"{self.out_dirs.get('y')}/layer_info_{self.lyr_no}.yaml"
        with open(layer_info_yaml, "w") as f:
            yaml.dump(fields_json, f)
        fields = [f['name'] for f in fields_json['fields'] if f['name'] not in UNWANTED_FIELDS]
        clogger.info(f"Fields: {fields}")
        return fields

    def fetch_ids_concurrently(self, query_params, step) -> T.Tuple[list, np.array]:
        worker_threads = min(100, (os.cpu_count() or 4) * 5)
        failed_urls = []
        new_ids = []

        with TPE(worker_threads) as ex:
            req_futures = []
            for offset in range(0, self.tot_service_rec, step):
                query_params['resultOffset'] = offset
                offset_query_url = f"{self.query_url}?{urllib.parse.urlencode(query_params)}"
                req_futures.append(ex.submit(fetch_json, offset_query_url))

            for future in tqdm(as_completed(req_futures), total=len(req_futures),
                               desc=f"w-{worker_threads} Fetching IDs", colour="yellow"):
                json_result = future.result()
                if 'objectIds' in json_result:
                    id_arr = np.array(json_result['objectIds'], dtype=np.int32)
                    new_ids.append(id_arr)
                elif 'features' in json_result:
                    id_arr = np.array([f['attributes'].get(self.id_field) for f in json_result['features']],
                                       dtype=np.int32)
                    new_ids.append(id_arr)
                else:
                    failed_urls.append(json_result)
                    continue
            if new_ids:
                feat_ids = np.concatenate(new_ids)
            else:
                feat_ids = np.array([], dtype=np.int32)

        logger.info(f'Failed URLs: {failed_urls}')
        logger.info(f'New IDs: {len(feat_ids)}')

        return failed_urls, feat_ids

    def _query_map_service_for_features(self,
                                        fields: T.Union[str, list]='*', ) -> list:

        # Define the query endpoint
        clogger.info(f"Query URL: {self.query_url}")

        if self.tot_service_rec == 0:
            count_params = {
                "where": self.where,
                "returnCountOnly": True,
                "f": "json"
            }
            tot_records_req = requests.get(self.query_url, params=count_params)
            clogger.info(f"ID Count URL: {tot_records_req.url}")
            tot_records_json = tot_records_req.json()
            self.tot_service_rec = int(tot_records_json["count"])
            clogger.info(f"Total Records: {self.tot_service_rec}")

        # Determine the step size for pages
        step = min(self.get_step_size(), 100)
        no_steps = self.tot_service_rec // step
        clogger.info(f'Step Size: {step}, Number of Steps: {no_steps}')

        # Get download to-do subsets
        # no_extant, to_do_len, subset_files = self._create_download_todo(step)

        #clogger.info(f'{to_do_len} features to download in batches of {step}')

        # Define query parameters to get records
        query_params = self.construct_features_query_params(step, fields)

        # Fetch features concurrently
        failed_urls = self.fetch_features_concurrently(query_params, step)

        return failed_urls

    def fetch_features_concurrently(self, query_params, step) -> list:
        # Loop through each page of query results concurrently
        worker_threads = 100
        failed_urls, disk_futures = [], []
        clogger.info(f'Using {worker_threads} threads to download  features '
                     f'in\nbatches of {step}-feature file subsets')
        with TPE(worker_threads) as ex, PPE(int(os.cpu_count() * 0.7)) as process_ex:
            pbar_req = tqdm(total=int(2 * self.tot_service_rec / step), desc=f"Qry Srvc", position=0, colour="yellow")
            pbar_feats = tqdm(total=self.tot_service_rec, desc="Tracking Features Saved", position=2, colour="yellow")

            req_futures = []
            for offset in range(0, self.tot_service_rec + 1, step):
                # Create each subset query
                query_params['resultOffset'] = offset

                url = f"{self.query_url}?{urllib.parse.urlencode(query_params)}"

                req_futures.append(
                    ex.submit(request, url, timeout=60))

            # Compile all futures once they're complete
            batch_size = worker_threads
            current_batch = []  # List to store GeoJSON features temporarily
            fut_recd, fut_prcd = 0, 0
            total_futures = len(req_futures)
            local_folder = self.out_dirs.get("00")

            def process_batch(batch):
                gs = [g for g in batch if 'features' in g]
                for feat_list in process_ex.map(get_features, gs):
                    # Save the GeoDataFrame to disk
                    batch_path = f"{local_folder}/batch_{len(feat_list)}_01.geojson"
                    batch_path = increment_file_naming(batch_path)
                    return features_to_disk(feat_list, batch_path)

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
                    process_batch(current_batch)
                    current_batch = []

                pbar_req.update(1)
                fut_prcd += 1

            # Handle any remaining data in `current_batch` after the loop
            if current_batch:
                process_batch(current_batch)
                pbar_req.update(1)
                fut_prcd += 1

            # Wait for all the saved futures to complete
            for i, f in enumerate(as_completed(disk_futures)):
                outpath, count, ids_downloaded = f.result()
                pbar_feats.update(count)
                pbar_feats.set_postfix({"Feats Saved": self.new_features,
                                        "Files Saved": i + 1})
                self.new_features += count

        return failed_urls

    def fetch_rows_concurrently(self, query_params, step) -> list:
        # Loop through each page of query results concurrently
        worker_threads = 100
        failed_urls, disk_futures, req_futures = [], [], []
        clogger.info(f'Using {worker_threads} threads to download  rows '
                     f'in\nbatches of {step}-feature file subsets')
        with TPE(worker_threads) as ex, PPE(int(os.cpu_count() * 0.7)) as process_ex:

            # Use offset to get all records
            for offset in range(0, self.tot_service_rec, step):
                query_params['resultOffset'] = offset
                offset_query_url = f"{self.query_url}?{urllib.parse.urlencode(query_params)}"
                req_futures.append(
                    ex.submit(fetch_json, offset_query_url))

            # Compile all futures once they're complete
            batch_size = worker_threads
            current_batch = []  # List to store JSON features temporarily
            fut_recd, fut_prcd = 0, 0
            local_folder = self.out_dirs.get("00")
            failed_urls = []

            def process_batch(batch):
                gs = [g for g in batch if 'features' in g]
                for feat_list in process_ex.map(get_features, gs):
                    # Save the JSON to disk
                    batch_path = f"{local_folder}/batch_{len(feat_list)}_01.json"
                    batch_path = increment_file_naming(batch_path)
                    return features_to_disk(feat_list, batch_path)

            # Use `as_completed` to process futures as they finish
            for future in as_completed(req_futures):
                fut_recd += 1
                result = future.result()
                # If the result is a GeoJSON feature collection, add it to the current batch
                if 'features' in result:
                    current_batch.append(result)
                else:
                    failed_urls.append(result)
                # If the batch size is reached, save to disk and clear the batch
                if len(current_batch) >= batch_size:
                    # Concatenate the GeoJSON features into a GeoDataFrame
                    process_batch(current_batch)
                    current_batch = []
                fut_prcd += 1

            # Handle any remaining data in `current_batch` after the loop
            if current_batch:
                process_batch(current_batch)
                fut_prcd += 1

        return failed_urls


    def _add_state_columns(self):

        # Get all the GEOJSON files
        origin_folder = self.out_dirs.get("00")
        file_list = [f"{origin_folder}/{f}" for f in os.listdir(origin_folder) if f.endswith('.geojson')]

        # Add state columns
        clogger.info(f'Starting Column Additions')
        state_path_futs, out_paths = [], []
        with PPE(int(os.cpu_count() * 0.70)) as pex:  # PPE(int(os.cpu_count() * 0.85)) as proc_ex:
            for path in file_list:
                state_path_futs.append(
                    pex.submit(self.add_state_column, path))

            # Add states columns to each GeoDataFrame
            pbar = tqdm(total=1 * len(file_list), desc="Adding State Column", colour="yellow")
            for f in as_completed(state_path_futs):
                pbar.update(1)
                path_with_states = f.result()
                out_paths.append(path_with_states)
        pex.shutdown()

    def collect_table_values(self, field_names: list):
        # Create folders
        self._create_folders()

        # Query service for all values
        service_fields = self.fetch_fields()
        fields_to_get = ', '.join([f for f in field_names if f in service_fields])

        self.tot_service_rec = self.get_total_record_count()

        query_params = self.construct_rows_query_params(step=100, fields=fields_to_get)
        clogger.info(f"Query Params: {query_params}")

        failed_urls = self.fetch_rows_concurrently(query_params, 100)

        return failed_urls

    def get_features_from_service(self):

        # Create folders
        self._create_folders()

        # Test connections
        if not self.connect_test:
            raise ValueError(f"Service at {self.query_url} is not connected")

        # # Get all the IDs from the service
        # self.stopwatch.log(f"Getting Service IDs ({self.id_field})")
        # failed_urls, id_count = self._query_map_service_for_ids()
        # self.stopwatch.log(f"Service IDs ({self.id_field}): {id_count}")
        # clogger.info(f"Failed URLs: {failed_urls}")
        # clogger.info(f"ID Count: {id_count}")

        # Get the offset or extant IDs
        extant_file_count, extant_feat_count = self._get_extant_ids()
        clogger.info(f"Extant Files: {extant_file_count}")
        clogger.info(f'Existing Features: {extant_feat_count}')

        # Get the features from service
        self.stopwatch.log(f"Getting Service Features")
        failed_urls = self._query_map_service_for_features(fields='*')
        self.stopwatch.log(f"Service Features: {self.new_features}")

        # Save failed URLs to a file
        failed_yaml = f"{self.out_dirs["y"]}/failed_urls_{self.lyr_no}.yaml"
        with open(failed_yaml, "w") as f:
            yaml.dump(failed_urls, f)

        # Add state columns to the GeoJSON files
        self.stopwatch.log(f"Adding State Columns")
        self._add_state_columns()
        self.stopwatch.log(f"State Columns Added")


if __name__ == "__main__":
    base_url = EFFECTIVE_NFHL_URL
    output_dir = "../test_data/downloads/nfhl/TABLES"
    state_shp = r"E:\carto\boundaries\states_FEMA.shp"
    # [0, 1, 2, 3, "offset", "extant_ids", "effective_ids"]
    nfhl_layer_number = 28
    state_to_get = None  # "IA"
      # "FLD_AR_ID LIKE '19%'"
    state_gdf = open_fc_any(state_shp)
    query_gdf = state_gdf.loc[state_gdf["STATE_FIPS"] == "19"]
    if not query_gdf.crs == 4269:
        logger.info(f"Converting query geometry to 4269 from {query_gdf.crs}")
        query_gdf = query_gdf.to_crs(4269)

    query_bounds = gBounds.from_gdf(query_gdf)
    dn = Download_NFHL(base_url, output_dir, nfhl_layer_number, state_to_get, query_geometry=query_bounds)
    dn._add_state_columns()

    # dn.collect_table_values(field_names=['FLD_AR_ID', 'OBJECTID', 'GlobalID', 'DFIRM_ID'])

    # Create the query string
    # query_conditions = " AND ".join([f"DFIRM_ID NOT LIKE '{fips}%'" for fips in state_fips_list])
    # query_string = f"({query_conditions})"