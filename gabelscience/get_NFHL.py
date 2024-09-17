import dask_geopandas
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import dask.distributed
import requests
import numpy as np
import pandas as pd
import time
import os
import geopandas as gpd
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
import ast


def setup_logger():
    # Create a custom logger
    module_name = __name__ if __name__ != "__main__" else os.path.splitext(os.path.basename(__file__))[0]
    logger = logging.getLogger(module_name)
    # Set the level of this logger. DEBUG is the lowest severity level.
    logger.setLevel(logging.DEBUG)
    # Create handlers
    file_handler = logging.FileHandler(os.path.join(os.getcwd(), f'{module_name}.log'))
    # Create formatters and add it to handlers
    log_fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_fmt)
    # Add handlers to the logger
    logger.addHandler(file_handler)
    return logger


# then call this function:
logger = setup_logger()

effective_nfhl = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer?"


def make_request(url, params, max_retries=3, delay=2) -> dict | None:
    if "query" not in url:
        logger.error(f"URL does not contain 'query': {url}")
        return None
    for attempt in range(max_retries):
        try:
            req_start = time.time()
            response = requests.get(url, params=params)
            logger.debug(f"Request time: {time.time() - req_start}, {response.url}")
            logger.debug(f"Request: {response.url}")
            response.raise_for_status()
            try:  # Raise an HTTPError for bad responses (4xx and 5xx)
                return response.json()
            except json.JSONDecodeError as e:
                logger.error(f"JSON Decode Error: {e}")
                logger.error(url)
                logger.error(params)
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            logger.error(url)
            if attempt < max_retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                return None
        except requests.exceptions as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            logger.error(url)
            if attempt < max_retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                return None


def get_count_params(where_clause='1=1'):
    return {'where': where_clause,
            'text': '',
            'objectIds': '',
            'time': '',
            'geometry': '',
            'geometryType': 'esriGeometryEnvelope',
            'inSR': '',
            'spatialRel': 'esriSpatialRelIntersects',
            'relationParam': '',
            'outFields': '',
            'returnGeometry': 'true',
            'returnTrueCurves': 'false',
            'returnIdsOnly': 'true',
            'returnCountOnly': 'true',
            'returnZ': 'false',
            'returnM': 'false',
            'returnDistinctValues': 'false',
            'returnExtentOnly': 'false',
            'featureEncoding': 'esriDefault',
            'f': 'pjson'}


def get_feat_params_from_oid_list(oid_list, fields=None):
    oid_string = ", ".join(str(oid) for oid in oid_list)
    fields = ", ".join(fields)
    return {'where': '1=1',
            'text': '',
            'objectIds': oid_string,
            'time': '',
            'geometry': '',
            'geometryType': 'esriGeometryEnvelope',
            'inSR': '',
            'spatialRel': 'esriSpatialRelIntersects',
            'relationParam': '',
            'outFields': fields,
            'returnGeometry': 'true',
            'returnTrueCurves': 'false',
            'returnIdsOnly': 'false',
            'returnCountOnly': 'false',
            'returnZ': 'false',
            'returnM': 'false',
            'returnDistinctValues': 'false',
            'returnExtentOnly': 'false',
            'featureEncoding': 'esriDefault',
            'f': 'geojson'}


def convert_dtype(gdf, target_dtypes):
    # Convert columns to target dtypes
    for col, dtype in target_dtypes.items():
        if col in gdf.columns:
            if dtype == 'geometry':
                gdf[col] = gdf[col].astype('geometry')
            else:
                gdf[col] = gdf[col].astype(dtype)
    return gdf


def get_oid_actuals(where_clause="1=1", offset=0, n=500):
    return {
        'where': where_clause,
        'text': '',
        'objectIds': '',
        'time': '',
        'geometry': '',
        'geometryType': 'esriGeometryEnvelope',
        'inSR': '',
        'spatialRel': 'esriSpatialRelIntersects',
        'distance': '',
        'units': 'esriSRUnit_Foot',
        'relationParam': '',
        'outFields': '',
        'returnGeometry': 'true',
        'returnTrueCurves': 'false',
        'maxAllowableOffset': '',
        'geometryPrecision': '',
        'outSR': '',
        'havingClause': '',
        'returnIdsOnly': 'true',
        'returnCountOnly': 'false',
        'orderByFields': '',
        'groupByFieldsForStatistics': '',
        'outStatistics': '',
        'returnZ': 'false',
        'returnM': 'false',
        'gdbVersion': '',
        'historicMoment': '',
        'returnDistinctValues': 'false',
        'resultOffset': f'{offset}',
        'resultRecordCount': f'{n}',
        'returnExtentOnly': 'false',
        'datumTransformation': '',
        'parameterValues': '',
        'rangeValues': '',
        'quantizationParameters': '',
        'featureEncoding': 'esriDefault',
        'f': 'geojson'}


DTYPES_SFHA = {
    'ALAND': 'int64',
    'DEP_REVERT': 'int64',
    'AR_SUBTRV': 'string',
    '__null_dask_index__': 'int64',
    'LEN_UNIT': 'string',
    'V_DATUM': 'string',
    'ZONE_SUBTY': 'string',
    'index_right': 'int64',
    'STATIC_BFE': 'float64',
    'NAME': 'string',
    'STATEFP': 'string',
    'SFHA_TF': 'string',
    'AR_REVERT': 'string',
    'GEOID': 'string',
    'BFE_REVERT': 'int64',
    'FLD_AR_ID': 'string',
    'STUDY_TYP': 'string',
    'geometry': 'geometry',
    'STATENS': 'string',
    'LSAD': 'string',
    'DUAL_ZONE': 'string',
    'DFIRM_ID': 'string',
    'STUSPS': 'string',
    'AFFGEOID': 'string',
    'DEPTH': 'float64',
    'AWATER': 'int64',
    'SOURCE_CIT': 'string',
    'VELOCITY': 'float64'}

WHERE_CLAUSES = {"SFHA": "SFHA_TF = 'T'", }

UNNEEDED_FIELDS = ["OBJECTID", "GFID", "SHAPE", "SHAPE.STArea()", "SHAPE.STLength()", "GLOBALID"]

class GetESRIGeoJSON:

    def __init__(self, update_layer, sub_type=None):
        self.out_folder = "./FEMA_data_test/"
        self.states_shp = "../references/ref_data/State_Bounds_Census.shp"
        self.lock = Lock()
        self.collection_response = requests.post(effective_nfhl, params={"f": "pjson"})
        self.collection_json = self._get_collection_json()
        # print(self.collection_json)

        self.layer_lookup = self._get_all_layers()

        self.lyr_no = self.layer_lookup[update_layer]
        self.lyr_dl_folder = self.out_folder + "raw_downloads/" + f"dl_{self.lyr_no}"
        self.lyr_folder = self.out_folder + f"filtered/" + f"S_{self.lyr_no}"

        self.subtype = sub_type
        self.lyr_page_base = f"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/{self.lyr_no}/"
        self.lyr_info = requests.get(self.lyr_page_base, params={"f": "pjson"}).json()

        self.lyr_fieldnames = [f["name"] for f in self.lyr_info['fields'] if f["name"] not in UNNEEDED_FIELDS]

        self.all_bounds, self.epsg = self._spatial_specs(self.lyr_info)
        self.feature_count = self._get_feat_count()
        print(f'{update_layer}, {self.subtype} has {self.feature_count} features')

        self.oid_field_name = None
        self.all_oids = []
        self.states_gdf, self.states_crs = self._init_states_gdf()

    def _get_collection_json(self):
        print(f"Main Collection: {self.collection_response.url}")
        if self.collection_response.status_code == 200:
            return self.collection_response.json()
        else:
            print(f'No response from {self.collection_response.status_code}')
            return None

    def _get_all_layers(self):
        all_dl_folders = []
        for root, dirs, files in os.walk(self.out_folder):
            for d in dirs:
                all_dl_folders.append(os.path.join(root, d))
        layer_lookup = {}
        if self.collection_response.status_code == 200:
            to_json = self.collection_response.json()
            print(f"Json: {to_json}")
            layers = to_json['layers']
            for lyr in layers:
                layer_lookup[lyr['name']] = str(lyr['id'])
                path = os.path.join(self.out_folder, "raw_downloads", f"dl_{lyr['id']}")
                normalized_path = os.path.normpath(path)
                os.makedirs(normalized_path, exist_ok=True)

        return layer_lookup

    @staticmethod
    def _spatial_specs(json_info):
        sr_stuff = json_info['sourceSpatialReference']
        epsg = int(sr_stuff['wkid'])
        xmin, xmax, ymin, ymax = (json_info['extent']['xmin'], json_info['extent']['xmax'],
                                  json_info['extent']['ymin'], json_info['extent']['ymax'])

        return (xmin, xmax, ymin, ymax), epsg

    def _get_feat_count(self):
        new_count = None
        total_wait = 300
        pbar = tqdm(total=total_wait)
        retries_per, delay_per = 3, 2
        start_time = time.time()
        while new_count is None and (elapsed := time.time() - start_time) < total_wait:
            count_response = make_request(self.lyr_page_base + "query",
                                          params=get_count_params(
                                              where_clause=WHERE_CLAUSES.get(self.subtype, '1=1')),
                                          max_retries=retries_per, delay=delay_per)
            pbar.update(int(elapsed))
            pbar.set_postfix({"Elapsed": f"{elapsed:.1f}s", "Count": new_count})
            if count_response and "count" in count_response:
                new_count = int(count_response['count'])
                pbar.update(total_wait)
                break

        print(f"New Count: {new_count}")
        return new_count

    def _init_states_gdf(self):
        state_gdf = gpd.read_file(self.states_shp)
        print(f'{len(state_gdf)} state geometries')
        return state_gdf, state_gdf.crs

    def get_all_oids(self):
        layer_stats = self.out_folder + "lyr_stats.json"
        oid_dump = f"./FEMA_data_test/oids_{self.lyr_no}_{self.subtype}.json"
        pbar = tqdm(total=self.feature_count, desc="Getting Object IDs from json")
        if os.path.exists(oid_dump):
            old_count = 0
            with open(oid_dump, 'r') as f:
                oid_lists = json.load(f)
                for l in oid_lists:
                    old_count += len(l)
                    pbar.update(len(l))
        else:
            old_count = 0

        # DO new OID lists
        if int(old_count) < 5:  # int(self.feature_count):
            print(f"Old Count: {old_count}, New Count: {self.feature_count}")
            futures = []
            post_dict = {"OIDS": 0, "TODO": self.feature_count}
            with ThreadPoolExecutor(20) as executor:
                pbar = tqdm(total=1 * self.feature_count, desc="Getting OIDs")
                pbar.set_postfix(post_dict)
                for i in range(0, self.feature_count, 500):
                    params = get_oid_actuals(where_clause=WHERE_CLAUSES.get(self.subtype, "1=1"), offset=i)
                    futures.append(executor.submit(self.query_json, self.lyr_page_base,
                                                   kwargs={"geojson": False, "param_pass": params}))

                for f in as_completed(futures):
                    self.lock.acquire()
                    try:
                        oid_dict = f.result()
                    except Exception as e:
                        logger.warning(f'{e} exception...')
                    else:
                        try:
                            if not self.oid_field_name:
                                self.oid_field_name = oid_dict.get("objectIdFieldName", None)
                            self.all_oids.append(oid_dict.get("objectIds", None))
                        finally:
                            self.lock.release()
                        post_dict["OIDS"] += 500
                        post_dict["TODO"] = self.feature_count - post_dict["OIDS"]
                        pbar.set_postfix(post_dict)
                    pbar.update(500)

            with open(oid_dump, 'w') as f:
                json.dump(self.all_oids, f)
            with open(layer_stats, "w") as j:
                max_count = max(self.feature_count, old_count)
                json.dump({self.lyr_no: {self.subtype: {"count": self.feature_count, "max count": max_count}}}, j)
        else:
            with open(oid_dump, 'r') as f:
                self.all_oids = json.load(f)

    @staticmethod
    def query_json(base_url, kwargs):
        total_wait = 300
        retries_per, delay_per = 5, 1
        start_time, elapsed = time.time(), 0
        feat_count = 0
        f_response = None
        geo = kwargs.get("geojson", False)
        # print(kwargs)

        if not geo:
            params = kwargs.get("param_pass", None)
            # print(f'Params: {params}')
        else:
            oid_list = kwargs.get("oid_list", None)
            params = get_feat_params_from_oid_list(oid_list, fields=kwargs.get("fields", None))
        while feat_count == 0 and elapsed < total_wait:
            f_response = make_request(base_url + "query",
                                      params=params,
                                      max_retries=retries_per, delay=delay_per)
            if f_response:
                if "features" in f_response:
                    feat_count = len(f_response['features'])
                elif "count" in f_response:
                    feat_count = f_response['count']
                elif "objectIds" in f_response:
                    feat_count = len(f_response['objectIds'])
            elapsed = time.time() - start_time
        if f_response:
            return f_response
        else:
            logger.warning(f'Failed on {params}')
            return None

    def get_features(self, offset, n_workers=20):

        to_get = self.all_oids[offset:offset + n_workers]
        futures = []
        index = offset
        with ThreadPoolExecutor(n_workers) as executor:
            for oid_list in to_get:
                futures.append(executor.submit(self.query_json,
                                               base_url=self.lyr_page_base,
                                               kwargs={"geojson": True, "oid_list": oid_list,
                                                       "fields": self.lyr_fieldnames}))
                index += 1

            wrote_count = 0
            for i, f in enumerate(as_completed(futures)):
                if f:
                    j = f.result()
                    if j is not None:
                        # print(f"Geometry: {j['features'][0]['geometry']}")
                        columns = ['geometry'] + list(j['features'][0]['properties'].keys())
                        gdf = gpd.GeoDataFrame.from_features(j, crs=self.epsg, columns=columns)
                        gdf = convert_dtype(gdf, DTYPES_SFHA)
                        if self.subtype == "SFHA":
                            gdf = gdf[gdf['SFHA_TF'] == 'T']
                        gdf = gdf.set_crs(epsg=self.epsg)
                        d_gdf = dask_geopandas.from_geopandas(gdf)
                        wrote_count += len(gdf)
                        yield d_gdf

    @staticmethod
    def init_dict(columns: list) -> dict:
        columns.append("state_fips")
        return {c: [] for c in columns}

    def state_sorter(self, gdf):
        # Convert states to df
        if isinstance(self.states_gdf, gpd.GeoDataFrame):
            self.states_gdf = dask_geopandas.from_geopandas(self.states_gdf, npartitions=len(self.states_gdf))
        intsct = dask_geopandas.sjoin(gdf, self.states_gdf, predicate="intersects")

        return intsct

    @staticmethod
    def write_subset_to_shp(gdf, column, cvalue, out_loc=os.getcwd()):
        to_write = gdf[gdf[column] == cvalue]
        if len(to_write) > 0:
            outpath = out_loc + f'/{column}_{cvalue}.shp'
            to_write.to_file(outpath, driver='ESRI Shapefile')
            return outpath
        else:
            return None

    def write_gdf(self, gdf, cvalue, out_loc=os.getcwd()):
        if len(gdf) > 0:
            outpath = out_loc + f'/S_{self.lyr_no}_{self.subtype if self.subtype else ""}_ST{cvalue}.geojson'
            if not os.path.exists(outpath):
                gdf.to_file(outpath, driver='GeoJSON')
            return outpath
        else:
            return None

    def get_layer(self):
        # Get object ids
        self.get_all_oids()

        # DO nested counting
        cd_gdf = None
        n_workers = 100
        to_collect = self.feature_count
        oid_length_max = max(len(l) for l in self.all_oids)
        print(f'Getting features in batches (of {oid_length_max}) * {n_workers} parallel workers at a time...')

        # Create state-sorted layer generator
        pbar = tqdm(total=n_workers * 3 * len(self.all_oids), desc=f"Collecting Features for {self.lyr_no} "
                                                                   f"{self.subtype if self.subtype else ''}")
        postfix_dict = {"Total": len(self.all_oids), "Collect": 0, "Concat": 0, "Write": 0}
        states_holder = []
        delayed_writes = []
        for n in range(0, len(self.all_oids), n_workers):
            # Yield each dask geodataframe
            for d_gdf in self.get_features(offset=n, n_workers=n_workers):
                postfix_dict["Collect"] += 1
                pbar.set_postfix(postfix_dict)
                pbar.update(1)

                new_intersection = self.state_sorter(d_gdf)
                states_holder.append(new_intersection)

            # Concatenate each n_workers # of dask geodataframes
            cd_gdf = dd.concat(states_holder, axis=0, join="outer", interleave_partitions=True)
            postfix_dict["Concat"] = n + n_workers
            states_holder = [cd_gdf]
            pbar.update(n_workers)
            pbar.set_postfix(postfix_dict)

        #     # Write intermediates
        #     outpath = self.out_folder + "concats/" + f"concat_{self.lyr_no}_{self.subtype}.hdf"
        #     # os.makedirs(outpath, exist_ok=True)
        #     delayed_writes.append(cd_gdf.to_hdf(outpath, key=str(n), compute=False))
        #     pbar.update(n_workers)
        #     postfix_dict["Write"] += n_workers
        #
        # with ProgressBar():
        #     for dw in delayed_writes:
        #         dw.compute()

        # Group and summarize
        grouped = cd_gdf.groupby(['STATEFP'])
        outpaths = {}
        # Extract unique STATEFP values
        unique_statefps = grouped['STATEFP'].unique().compute()
        # Convert the unique values to a list of strings
        states_to_export = [str(state[0]) for state in unique_statefps]
        print(f'States: {states_to_export}')
        time.sleep(1)

        # Export states
        pbar = tqdm(total=len(states_to_export), desc="Exporting States")
        for state in states_to_export:
            state_gdf = cd_gdf[cd_gdf['STATEFP'] == state].compute()
            # print(f'\nOutput GDF: {state_gdf}')
            outpath = self.write_gdf(state_gdf, cvalue=state, out_loc=self.out_folder)
            if outpath is not None:
                pbar.set_postfix({"Wrote": os.path.split(outpath)[1], "Total": len(states_to_export)})
                pbar.update(1)
                outpaths[state] = outpath


if __name__ == '__main__':
    subtype = None  # "SFHA"
    effect = GetESRIGeoJSON("FIRM Panels", sub_type=subtype)
    effect.get_layer()

    # effect.get_layer()