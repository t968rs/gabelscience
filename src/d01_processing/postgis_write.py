# src/d01_processing/postgis_write.py
import geopandas as gpd
from typing import Any, Union
from sqlalchemy import MetaData, Table
from tqdm import tqdm
from pyproj import CRS
from src.d00_utils.gbounds import gBounds
from src.d00_utils.open_spatial import open_fc_any
from src.d00_utils.postgis_helpers import *
from src.d01_processing.spatial_operations import set_geometry_column
import os




logger = getalogger("postgis_write", 10)


def create_postgis_table(engine,
                         table_name,
                         template_gdf,
                         srid=4269,
                         pgis_geom: Any ='GEOMETRY'):
    # Init the metadata
    metadata = MetaData()

    if isinstance(pgis_geom, str):
        pgis_geom = Geometry(pgis_geom.upper(), srid=srid, name='geometry')

    if "." in table_name:
        schema = table_name.split(".")[0]
        table_name = table_name.split(".")[1]
    else:
        schema = "public"

    # Define the input columns
    if isinstance(template_gdf, Union[gpd.GeoDataFrame, pd.DataFrame]):
        input_columns = df_columns_to_sql_columns(template_gdf, pgis_geom)
        table = Table(table_name, metadata, *input_columns)
    elif isinstance(template_gdf, dict):
        input_columns = template_gdf
        table = Table(table_name, metadata)
        for col_name, col_type in input_columns.items():
            if isinstance(col_type, str) and col_name.lower() != 'geometry':
                col_type = String
            elif col_name.lower() == 'geometry':
                col_type = pgis_geom

            table.append_column(Column(col_name, col_type))
    else:
        raise ValueError(f"Invalid template type: {type(template_gdf)}")

    logger.info(f"Creating table '{table_name}', SRID: {srid}, Geometry: {pgis_geom}, Columns: {input_columns}")

    # Check if table exists
    if not inspect(engine).has_table(table_name, schema=schema):
        # Define the table
        table.create(engine)
        logger.info(f"Table '{table_name}' created successfully.")
        return

    # Check for missing columns
    logger.info(f"Table '{table_name}' already exists.")

    extant_columns = get_p_Columns(engine, table_name)
    missing, diffs = get_p_column_differences(extant_columns, input_columns)
    if missing:
        logger.info(f"Table '{table_name}' exists but is missing columns: {missing}")
        for col in input_columns:
            if col.name in missing:
                add_p_column(engine, table_name, col)
                logger.info(f"Added {col} '{table_name}'.")
    if diffs:
        logger.info(f"Table '{table_name}' has type mismatches: {diffs}")
        for col_name, (existing_type, new_type) in diffs.items():
            change_p_column_type(engine, table_name, col_name, new_type)
        logger.info(f"Updated column types in '{table_name}'.")

def check_geometry_registered(engine, table_name):
    with engine.connect() as conn:

        query = text('SELECT * FROM geometry_columns WHERE f_table_name = :table_name')
        result = conn.execute(query, {"table_name": table_name})
        logger.info(f"Geometry Columns: {result}")

    if result:
        fetched = result.fetchone()
        column_names = result.keys()

        return dict(zip(column_names, fetched))
    return None


def create_temp_schema(geom_name, table_name, db_name, geo_type='Polygon',
                       table_columns=None, crs=4269):
    """
    Writes a GeoDataFrame partition to a PostGIS table with retry logic.

    Parameters:
    - partition (GeoDataFrame): The GeoDataFrame partition to write.
    - table_name (str): The target PostGIS table name.
    - db_name (str): The name of the PostGIS database.
    - geo_type (str): The geometry type of the partition.
    - table_columns (dict): The column names and types for the table.

    Returns:
    - None
    """
    logger.debug(f"\nCreating temp table schema and target table for '{table_name}'.")
    if "." in table_name:
        schema = table_name.split(".")[0]
    else:
        schema = "public"

    # Get geometry type conversions (GDF -> PostGIS)
    epsg = int(crs.to_epsg() if isinstance(crs, CRS) else crs)

    # Init the engine
    engine = get_postgis_engine(db_name)

    # Create the table (if needed)
    if not inspect(engine).has_table(table_name, schema=schema):
        create_postgis_table(engine, table_name, table_columns, srid=epsg, pgis_geom=geo_type)
        dtype = {geom_name: Geometry(geometry_type=geo_type.upper(), srid=epsg, )}

    else:
        # Check if geometry column registered in geometry_columns
        geom_registered = check_geometry_registered(engine, table_name)
        if geom_registered:
            logger.info(f"Geometry registered: {geom_registered}")
        # Create the dtype
        dtype = {geom_name: Geometry(geometry_type=geo_type.upper(), srid=epsg, )}
    logger.info(f'Partition Geometry: {geom_name}, PostGIS Geometry: {dtype[geom_name]}')

    temp_schema = table_name + "_temp"
    temp_schema = create_new_schema(engine, temp_schema)

    return temp_schema, dtype, table_name

def write_partition_to_postgis(partition, table_name, db_name, temp_schema, geo_type='Polygon',
                               table_columns=None):
    """
    Writes a GeoDataFrame partition to a PostGIS table with retry logic.
    """
    partition_id = uuid.uuid4()

    # Get geometry type conversions (GDF -> PostGIS)
    parition_geom = partition.geometry.name
    epsg = int(partition.crs.to_epsg())
    part_gdf = gpd.GeoDataFrame(partition, crs=partition.crs, geometry=parition_geom)

    # Init the engine
    engine = get_postgis_engine(db_name)

    # Check if geometry column registered in geometry_columns
    geom_registered = check_geometry_registered(engine, table_name)
    if geom_registered:
        logger.info(f"Geometry registered: {geom_registered}")
        part_gdf = set_geometry_column(part_gdf, geom_registered['f_geometry_column'])

    for col in part_gdf.columns:
        if col not in table_columns:
            part_gdf = part_gdf.drop(columns=[col])

    logger.info(f"GDF Columns: {part_gdf.columns}")
    # Create the dtype
    dtype = {part_gdf.geometry.name: Geometry(geometry_type=geo_type.upper(), srid=epsg, )}

    temp_table_name = f"temp_table_{partition_id}"
    if not inspect(engine).has_table(table_name, schema=temp_schema):
        create_postgis_table(engine, table_name, table_columns, srid=epsg, pgis_geom=geo_type)

    part_gdf.to_postgis(
        name=temp_table_name,
        con=engine,
        if_exists='append',
        chunksize=10000,
        dtype=dtype,
        schema=temp_schema
    )
    logger.info(f"Partition successfully written to '{table_name}'.")


def get_table_subset(table_name, db_name, column_name, limit=None):
    """Query a PostGIS table for unique values in a column."""
    # Init the engine
    engine = get_postgis_engine(db_name)

    # Query the table
    with engine.connect() as conn:
        query = f"""SELECT "{column_name}" FROM "{table_name}";"""
        result = conn.execute(text(query)).fetchall()
    if limit:
        return [row[0] for row in result][:limit]
    return [row[0] for row in result]

def query_table_using_list(table_name, column_name, values):
    """Query a PostGIS table for rows matching specific values in a column."""

    return f"""
    SELECT *
    FROM "{table_name}"
    WHERE "{column_name}" IN ({', '.join([str(v) for v in values])});
    """

def spatial_intersect_to_gpd(table_name, db_name, geom_column, other_geometry,
                             limit=None):
    engine = get_postgis_engine(db_name)

    geom, epsg = geom_wkt_from_mixin(other_geometry)

    select_query = text(f'''
    SELECT *
    FROM "{table_name}"
    WHERE ST_Intersects("{geom_column}", ST_GeomFromText('{geom}'))''')

    if limit:
        select_query = text(f"{select_query} LIMIT {limit};")
    else:
        select_query = text(f"{select_query};")

    with engine.connect() as conn:
        gdf_chunks = gpd.read_postgis(str(select_query), con=conn, geom_col=geom_column, chunksize=500000)

        for gdf in gdf_chunks:
            if not gdf.crs:
                gdf.set_crs(CRS.from_user_input(epsg))

            yield gdf

def spatial_intersect_query(table_name, db_name, geom_column, other_geometry, limit=None):
    """Query a PostGIS table for rows intersecting with a geometry."""

    geom, epsg = geom_wkt_from_mixin(other_geometry)

    # Init the engine
    engine = get_postgis_engine(db_name)

    # Query the table
    with engine.connect() as conn:
        query = f"""
        SELECT *
        FROM "{table_name}"
        WHERE ST_Intersects("{geom_column}", ST_GeomFromText('{geom}'));
        """
        if limit:
            query += f" LIMIT {limit};"
        logger.info(f"Query: {query}")
        result = conn.execute(text(query)).fetchall()
    return result

def geom_wkt_from_mixin(other_geometry, crs=4269):
    # Get target table SRID
    target_crs = CRS.from_user_input(crs)

    geom = None

    # Get the geometry
    if isinstance(other_geometry, tuple):
        bounds = gBounds.from_bbox(other_geometry, target_crs)
        geom = bounds.to_wkt()
    elif isinstance(other_geometry, str):
        if not os.path.exists(other_geometry):
            geom = other_geometry
        else:
            try:
                gdf = open_fc_any(other_geometry)
                bounds = gBounds.from_gdf(gdf)
                geom = bounds.to_wkt()
            except Exception as e:
                logger.error(f"Invalid geometry path: {other_geometry}, {e}")
    elif isinstance(other_geometry, gpd.GeoDataFrame):
        bounds = gBounds.from_gdf(other_geometry)
        geom = bounds.to_wkt()
    else:
        try:
            geom = other_geometry.geometry.wkt
        except AttributeError as e:
            logger.error(f"Invalid geometry type: {type(other_geometry)}, {e}")
            raise e

    # Get epsg
    crs = geom.split("SRID=")[1].split(";")[0]
    epsg = CRS.from_user_input(crs).to_epsg()
    return geom, epsg

def get_srid_from_table(table_name, db_name):
    # Init the engine
    engine = get_postgis_engine(db_name)

    # Get the SRID
    with engine.connect() as conn:
        query = f"""
        SELECT Find_SRID('public', '{table_name}', 'geometry');
        """
        result = conn.execute(text(query)).fetchall()
    return result[0][0]

def export_table_to_shp(table_name, db_name, out_path, selection_column=None, selection_values=None):
    """
    Export a PostGIS table to a shapefile.

    Parameters:
    - table_name (str): The PostGIS table name to export.
    - db_name (str): The name of the PostGIS database.
    - out_path (str): The path to save the shapefile.
    - epsg (int): The EPSG code for the shapefile's CRS.

    Returns:
    - None
    """

    # Init the engine
    engine = get_postgis_engine(db_name)

    # Get the unique values
    if selection_column and selection_values:
        ex_query = query_table_using_list(table_name, selection_column, selection_values)
        values_to_get = get_table_subset(table_name, db_name, selection_column, limit=50)
    else:
        ex_query = f"""SELECT * FROM "{table_name}";"""
        values_to_get = None
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Read the table
    gdf = gpd.read_postgis(
        f"""SELECT * FROM "{table_name}";""",
        con=engine,
        geom_col='geometry',
    )

    # Write the shapefile
    gdf.to_file(out_path)
    logger.info(f"Table '{table_name}' exported to shapefile: {out_path}.")


if __name__ == "__main__":
    # Test the functions
    database_name = "nfhl_tr32"
    # geopath = r"E:\automation\toolboxes\gabelscience\test_data\gjson_concat_test\input\batch_785100.geojson"
    # logger.info(f"GeoPath: {geopath}")
    # ingdf = gpd.read_file(geopath).explode(ignore_index=True)
    # logger.info(f"--GeoDataFrame: \n{ingdf.head()}")
    # write_gdf_to_postgis(ingdf, "batch_785100", database_name, geo_type='POLYGON')
    state_shp = r"E:\carto\boundaries\states_FEMA.shp"
    state_gdf = open_fc_any(state_shp)
    print(f'CRS: {state_gdf.crs}')
    iowa_gdf = state_gdf.loc[state_gdf["STATE_ABBR"] == "IA"]
    print(f'Selecting GDF: \n{iowa_gdf.head()}')

    table_srid = get_srid_from_table("S_FLD_HAZ_AR", database_name)
    print(f"Table SRID: {table_srid}")
    pbar = tqdm(total=0, desc="Exporting to GPKG")
    for sel_gdf in spatial_intersect_to_gpd("S_FLD_HAZ_AR", database_name, "geometry", iowa_gdf):
        pbar.total += len(sel_gdf)
        pbar.update(len(sel_gdf))
        sel_gdf.to_file(r"E:\nfhl\IA\NFHL_from_SRV\IA_NFHL.gpkg", driver='GPKG', layer="S_FLD_HAZ_AR",
                        mode='a')


# https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&productID=NFHL_19_20240722
# https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&productID=NFHL_01_20241004
# https://msc.fema.gov/portal/advanceSearch