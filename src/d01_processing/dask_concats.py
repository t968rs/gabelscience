import os
import warnings
import dask
import dask_geopandas as dgpd
import geopandas as gpd
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import Union, List
from dask import dataframe as dd
from dask.distributed import Client, performance_report as dpr
from dask.diagnostics import ProgressBar
from references.fema_tables import FEMATable
from references.fema_field_definitions import convert_fields_to_postgres_types
from src.d00_utils.file_typing import gFileType
from src.d00_utils.maths import get_random_sample
from src.d00_utils.loggers import getalogger
from src.d00_utils.postgis_helpers import get_list_of_records, check_p_table_exists
from src.d01_processing.postgis_write import create_temp_schema, write_partition_to_postgis
from src.d01_processing.spatial_operations import get_gdf_geometry_types, drop_empty_geometries, ensure_polygons

dask.config.set({'logging.distributed.shuffle': 'error'})


logger = getalogger("dask_concats")

GEOMETRY_TYPES = ['Point', 'Linestring', 'Polygon']

UNWANTED_COLUMNS = ["SHAPE.STArea()", "SHAPE.STLength()", "GFID", "SHAPE.STLength()"]


def filter_existing_ids(df, existing_ids, id_field):
    if df.empty:
        # If the partition is empty, return it as is or handle accordingly
        return df
    if id_field in df.columns:
        df = df[~df[id_field].isin(existing_ids)]
    return df


def read_json_any(path,
                  all_columns=None
                  ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    ftype = gFileType.from_path(path)
    if ftype.fcode == "geojson":
        df = gpd.read_file(path)
    elif ftype.fcode == "json":
        df = pd.read_json(path)
    else:
        raise ValueError(f"Unsupported file type: {ftype.code}")

    if all_columns is not None:
        # Add missing columns
        for col in all_columns:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[all_columns]  # Ensure consistent column order
    return df


def fix_geometries(gdf):
    gdf = gdf.explode(ignore_index=True)
    gdf['geometry'] = gdf['geometry'].buffer(0)
    return gdf

def convert_geometry_to_wkt(gdf):
    gdf['geom'] = gdf['geometry'].apply(lambda x: x.wkt)
    return gdf


def align_columns(df, all_columns=None, meta=None):
    """
    Aligns DataFrame columns based on all_columns and meta.
    """
    with warnings.catch_warnings():
        if all_columns is not None:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[all_columns]  # Ensure consistent column order

        if meta is not None:
            for col in meta.columns:
                if col not in df.columns:
                    df[col] = pd.NA
                # Apply type only if the column has non-NA values
                if not df[col].isna().all():
                    # Suppress the FutureWarning about dtype setting for all-NA columns
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", FutureWarning)
                        df.loc[:, col] = df[col].astype(meta[col].dtype)

            df = df[meta.columns]  # Ensure consistent column order

    return df

@dask.delayed
def read_geojson_lazy(path, all_columns=None, meta=None):
    df = gpd.read_file(path)
    return align_columns(df, all_columns, meta)

@dask.delayed
def read_json_lazy(path, all_columns=None, meta=None):
    df = read_json_any(path)
    if meta:
        return align_columns(df, all_columns, meta)
    return df


@dask.delayed
def read_batch_lazy(batch_paths, all_columns=None, meta=None):
    dfs = [align_columns(gpd.read_file(path), all_columns, meta) for path in batch_paths]
    if dfs:
        return gpd.GeoDataFrame(pd.concat(dfs, ignore_index=True))
    else:
        # Return empty GeoDataFrame with specified columns
        return gpd.GeoDataFrame(columns=all_columns)


def geo_col_last(df):
    columns = df.columns.tolist()
    if 'geometry' in columns:
        columns.remove('geometry')
        columns.append('geometry')
    return df[columns]


def concatenate_geojsons_dask(paths: List[str],
                              rows_per_part=None,
                              existing_ids_path=None,
                              id_field=None, meta=None):

    sample_size = min(5, len(paths))
    rows_per_path = int(max([gpd.read_file(p).shape[0] for p in get_random_sample(paths, sample_size)]))
    row_est = len(paths) * rows_per_path

    # dask.config.set(scheduler='threads')
    clogger = getalogger("concatenate_geojsons_dask")
    col_list = meta.columns.tolist()
    clogger.info(f'Meta Columns: {meta.columns}')
    clogger.info(f'Meta Geometry: {meta.geometry.name}')
    clogger.info(f'Meta CRS: {meta.crs}')
    clogger.info(f'Existing IDs Path: {existing_ids_path}')

    # Read existing IDs if provided
    existing_ids = set()
    if isinstance(existing_ids_path, Union[list, set]):
        existing_ids.update(existing_ids_path)
    elif existing_ids_path and os.path.exists(existing_ids_path):
        existing_ids.update(np.load(existing_ids_path))

    clogger.info(f'Existing IDs: {len(existing_ids)}')

    if rows_per_part:
        # Get batch size
        batch_size = int(rows_per_part // rows_per_path)
        print(f'\n\tRows per Path: {rows_per_path}, \n\tBatch Size: {batch_size}')

        # Batch the paths
        batches = [paths[i:i + batch_size] for i in range(0, len(paths), batch_size)]
        delayed_gdfs = [read_batch_lazy(batch, all_columns=col_list, meta=meta) for batch in batches]
    else:
        # Read all GeoJSON files lazily
        delayed_gdfs = [read_geojson_lazy(path, all_columns=col_list, meta=meta) for path in paths]

    # Convert delayed GDFs to DDF and then to GDDF
    ddf = dd.from_delayed(delayed_gdfs, meta=meta)
    dgdf = dgpd.from_dask_dataframe(ddf, geometry='geometry')
    dgdf = dgdf.astype(meta.dtypes.to_dict())

    # Apply filtering using map_partitions
    if existing_ids:
        dgdf = dgdf.map_partitions(filter_existing_ids, existing_ids=existing_ids, id_field=id_field)

    dgdf = dgdf.map_partitions(fix_geometries)

    return dgdf, row_est

def write_dask_to_geojson(dgdf, target_path, retries=3):
    """
    Writes a Dask GeoDataFrame to a GeoJSON file with retry logic.

    Parameters:
    - dgdf (dask_geopandas.GeoDataFrame): The Dask GeoDataFrame to write.
    - target_path (str): The target GeoJSON file path.
    - retries (int): Number of retry attempts on failure.

    Returns:
    - None
    """

    logger.info(f"Writing Dask GeoDataFrame to GeoJSON file '{target_path}'...")

    for attempt in range(1, retries + 1):
        try:
            # Step 8: Apply the write function to each partition
            with ProgressBar():
                gdf = dgdf.compute()
                gdf.to_file(target_path)# Trigger the computation
            logger.info(f"Successfully written to '{target_path}'.")
            break  # Success
        except Exception as e:
            logger.error(f"Attempt {attempt} failed to write partition to GeoJSON: {e}")
            if attempt < retries:
                logger.info("Retrying...")
            else:
                logger.error("All retry attempts failed.")
                raise e


class append_geoJSON_to_postGIS:
    def __init__(
            self,
            paths: list,
            rows_per_part: int = None,
            id_field: str = None,
            db_name: str = None,
            fema_table = None
    ):
        """
        Initializes the ConcatGeoJsons class with necessary parameters.

        Parameters:
        - paths (list): List of GeoJSON file paths to concatenate.
        - target (str): Target path or table name for the output.
        - rows_per_part (int, optional): Number of rows per Dask partition.
        - existing_ids_path (str, optional): Path to existing IDs file for filtering.
        - id_field (str, optional): Field name representing unique IDs in the data.
        - db_name (str, optional): Name of the PostGIS database to write to.
        """
        self.paths = paths
        self.rows_per_part = rows_per_part
        self.id_field = id_field
        self.db_name = db_name
        if not db_name:
            self.db_name = "gabelscience"

        self.geometry_subtype = None

        self.fema_table: str = fema_table
        self.meta = gpd.GeoDataFrame()

    def _gen_target_meta(self):
        """
        Generates a meta GeoDataFrame based on the target FEMA table.
        """

        # If sample_size is specified, sample the paths
        sample_pct = 0.1
        sample_size = max(int(1), int(len(self.paths) * sample_pct))
        sampled_paths = get_random_sample(self.paths, sample_size)

        crs_list = set()

        fema_table = FEMATable.from_table_type(self.fema_table.upper())
        logger.info(f"--Target for Concat: \n{fema_table.__repr__()}")

        # Get all columns and their dtypes from the sampled files
        columns = set()
        for path in sampled_paths:
            df = read_json_any(path)
            columns.update([c for c in df.columns if c not in UNWANTED_COLUMNS])
            if hasattr(df, 'crs') and df.crs or isinstance(df, gpd.GeoDataFrame):
                crs_list.add(df.crs)
        crs_list = list(crs_list)
        logger.info(f"CRS List: {crs_list}")

        if fema_table:
            meta_dict = fema_table.to_geopandas()
            logger.info(f"GPD Fields: {meta_dict}")
            meta_dict['geometry'] = gpd.GeoSeries([], dtype='geometry')

            # Add missing columns
            if self.id_field not in meta_dict:
                meta_dict[self.id_field] = pd.Series(dtype='int')
            for col in columns:
                if col not in meta_dict:
                    meta_dict[col] = pd.Series(dtype='object')
            logger.info(f"Generated target meta GeoDataFrame with columns: \n{meta_dict}")
            gdf = gpd.GeoDataFrame(meta_dict, geometry='geometry', crs=crs_list[0])
            logger.info(f"Generated target meta GDF with columns: \n{gdf}")
            return gdf
        else:
            raise ValueError("FEMA table not specified.")

    def _generate_meta_pathlist(self, paths, sample_pct=None):
        """
        Generates a meta GeoDataFrame that includes the union of all fields
        from a random sample of GeoJSON files.

        Parameters:
        - paths (list of str): List of file paths to GeoJSON files.
        - sample_size (int, optional): Number of files to sample. If None, uses all files.

        Returns:
        - meta (geopandas.GeoDataFrame): An empty GeoDataFrame with all columns and their dtypes.
        """

        # If sample_size is specified, sample the paths
        if not sample_pct:
            sample_pct = 0.1
        sample_size = max(int(1), int(len(paths) * sample_pct))
        sampled_paths = get_random_sample(paths, sample_size)

        all_columns = set()
        column_types = {}
        crs_list = set()

        logger.info(f"{self.fema_table.__repr__()}")

        # Get all columns and their dtypes from the sampled files
        print(f"Sampled Paths: {sampled_paths}")
        for path in sampled_paths:
            df = read_json_any(path)
            if hasattr(df, 'crs'):
                crs_list.add(df.crs)
            all_columns.update([c for c in df.columns if c not in UNWANTED_COLUMNS])
            for col in df.columns:
                if col not in UNWANTED_COLUMNS:
                    column_types[col] = df[col].dtype

        # Ensure 'geometry' is set as the geometry column
        if 'geometry' not in all_columns:
            raise ValueError("No geometry column found in the sampled files.")

        # Create an empty DataFrame with all columns and their dtypes
        crs_list = list(crs_list)
        most_common_crs = max(crs_list, key=crs_list.count)
        meta_dict = {}
        for col in all_columns:
            dtype = column_types.get(col, 'object')
            if col == 'geometry':
                meta_dict[col] = gpd.GeoSeries([], dtype='geometry')
            elif col in UNWANTED_COLUMNS:
                logger.info(f"Skipping unwanted column: {col}")
                continue
            else:
                meta_dict[col] = pd.Series(dtype=dtype)
        gdf = gpd.GeoDataFrame(meta_dict, geometry='geometry', crs=most_common_crs)
        gdf = gdf.drop(columns=[col for col in UNWANTED_COLUMNS if col in gdf.columns])
        logger.info(f"Generated meta GeoDataFrame with columns: \n{gdf.columns}")
        gdf = geo_col_last(gdf)
        return gdf

    def _determine_geometry_type(self):
        """
        Determines the geometry type of the GeoJSON files by sampling a subset of the data.
        """
        sample_paths = get_random_sample(self.paths, min(5, len(self.paths)))
        temp_ddf = dd.from_delayed([read_geojson_lazy(p) for p in sample_paths])
        logger.info(f"Determining geometry type from sampled data...{sample_paths}")

        # Check if temp_ddf is empty
        if temp_ddf.npartitions == 0:
            raise ValueError("The Dask DataFrame 'temp_ddf' is empty.")

        # Add logging to inspect the partitions
        logger.info(f"Number of partitions: {temp_ddf.npartitions}")

        # Check the first partition to ensure it has data
        part_number = 0
        first_partition = temp_ddf.partitions[part_number].compute()
        while first_partition.empty:
            part_number += 1
            if part_number >= temp_ddf.npartitions:
                break
            logger.warning(f"First partition is empty. Trying next partition ({part_number}...")
            first_partition = temp_ddf.partitions[part_number].compute()

        if first_partition.empty:
            raise ValueError("All partitions are empty.")

        # Define the expected output structure (metadata)
        meta = pd.Series(dtype='object')

        # Proceed with map_partitions
        geom_types = temp_ddf.partitions[part_number:].map_partitions(get_gdf_geometry_types, subset=0.1, meta=meta).compute()
        geom_types = list(set([gt for sublist in geom_types for gt in sublist]))
        for gt in geom_types:
            if gt not in GEOMETRY_TYPES:
                geom_types.remove(gt)
        if len(geom_types) > 1:
            raise ValueError(f"Multiple geometry types found in the data, {geom_types}")
        self.geometry_subtype = geom_types[0]
        logger.info(f"Geometry type determined: {self.geometry_subtype}")

    def get_existing_ids(self):
        """
        Get existing IDs from the target PostGIS table.

        """
        if not check_p_table_exists(self.db_name, self.fema_table):
            logger.info(f"Table '{self.fema_table}' not yet in '{self.db_name}'.")
            return set()
        # Query existing IDs from the target table
        existing_ids = get_list_of_records(self.db_name, self.fema_table, self.id_field)
        return set(existing_ids)

    def concat(self):
        """
        Concatenates the provided GeoJSON files and saves the result to either a PostGIS table
        or a GeoJSON file based on the estimated number of rows.

        Returns:
        - tuple: (target, row_est) where target is the output path or table name, and row_est is the row estimate.
        """
        # Get geometry subtype from the data
        self._determine_geometry_type()
        self.meta = self._gen_target_meta()
        existing_ids = self.get_existing_ids()

        try:
            logger.info("Starting concatenation of GeoJSON files.")
            dgdf, row_est = concatenate_geojsons_dask(
                self.paths,
                self.rows_per_part,
                existing_ids,
                self.id_field,
                meta=self.meta
            )
            with ProgressBar():
                dgdf = dgdf.persist()
                logger.info(f"Concatenation complete. Estimated rows: {row_est}.")

        except Exception as e:
            logger.error(f"An error occurred during concatenation and saving: {e}")
            raise e

        dgdf = dgdf.map_partitions(drop_empty_geometries)
        if self.geometry_subtype == 'POLYGON':
            dgdf = dgdf.map_partitions(ensure_polygons)
        return dgdf, row_est

    def save_postgis(self, dgdf):
        """
        Saves the concatenated GeoDataFrame to a PostGIS table.
        """
        logger.info(f"Writing Dask GeoDataFrame to PostGIS table '{self.fema_table}' as {self.geometry_subtype}...")

        if self.fema_table:
            post_gis_columns = convert_fields_to_postgres_types(self.fema_table)
        else:
            post_gis_columns = {}

        # Add missing columns
        post_gis_columns['geometry'] = 'GEOMETRY'
        for col in dgdf.columns:
            if col not in post_gis_columns:
                post_gis_columns[col] = 'TEXT'
        logger.info(f"Sending PostGIS Columns: {post_gis_columns}")

        # Create target table and temp schema
        logger.info(f"Creating target table '{self.fema_table}' in '{self.db_name}'...")
        logger.info(f'Creating temp schema for {self.fema_table}...')
        temp_schema, dtype, table_name = create_temp_schema(
            geom_name=dgdf.geometry.name,
            table_name=self.fema_table,
            db_name=self.db_name,
            geo_type=self.geometry_subtype,
            table_columns=post_gis_columns,
            crs=dgdf.crs
        )
        logger.info(f'Temp Schema: {temp_schema}')

        def write_partition(partition):

            write_partition_to_postgis(
                partition=partition,
                table_name=self.fema_table,
                db_name=self.db_name,
                temp_schema=temp_schema,
                geo_type=self.geometry_subtype,
                table_columns=post_gis_columns
            )

        pbar = tqdm(total=dgdf.npartitions, desc="Writing to PostGIS", unit="partitions")
        with ProgressBar():
            dgdf.map_partitions(write_partition, meta=gpd.GeoDataFrame()).compute()
            pbar.update(1)

if __name__ == "__main__":
    input_dir = r"E:\automation\toolboxes\gabelscience\test_data\downloads\nfhl\IA\28_attributes"
    # output_file = r"E:\automation\toolboxes\gabelscience\test_data\gjson_concat_test\output\S_FLD_HAZ_AR.geojson"

    # Get all GeoJSON files in the input directory
    geojson_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.geojson')]
    print(f'{len(geojson_files)} GeoJSON Files')

    # Concatenate all GeoJSON files into a single GeoDataFrame

    client = Client(n_workers=5, threads_per_worker=4, local_directory=r"E:\__DASK")

    try:
        with dpr(filename=f"./logs/dask-report.html"):
            _ = append_geoJSON_to_postGIS(geojson_files, 25_000, "OBJECTID",
                                          "nfhl_tr32", "S_FLD_HAZ_AR")
            outddf, number_rows = _.concat()
            logger.info(f"Number of Rows: {number_rows}")
            logger.info(f"Number of Partitions: {outddf.npartitions}")
            _.save_postgis(outddf)

    except Exception as e:
        client.close()
        raise e
    client.close()

