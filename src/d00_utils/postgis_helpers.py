from typing import Tuple, List, Dict
import pandas as pd
from geoalchemy2 import Geometry
from sqlalchemy import text, Column, Integer, Boolean, String, Float, create_engine, URL, inspect, \
    Double
import uuid
from src.d00_utils.loggers import getalogger

logger = getalogger("postgis_helpers", 30)


def spatial_index_queries(table_name, qtype='CREATE'):
    if qtype == 'CREATE':
        query_string = f"""
        CREATE INDEX IF NOT EXISTS '{table_name}'_geom_idx
        ON '{table_name}'
        USING GIST (geometry);
        """
    elif qtype == 'CHECK':
        query_string = f"""
        SELECT 1
        FROM pg_indexes
        WHERE tablename = '{table_name}' AND indexname = '{table_name}_geom_idx';
    """
    elif qtype == 'DROP':
        query_string = f"""
        DROP INDEX IF EXISTS '{table_name}_geom_idx';
        """
    else:
        raise ValueError(f"Invalid query type: {qtype}")
    return text(query_string)


def create_new_schema(engine, schema_name, temp=False):
    # Create the schema
    if temp:
        schema_name = f"{schema_name}_{uuid.uuid4().hex[:8]}"
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        conn.execute(text(f'SET search_path TO "{schema_name}", public'))
        conn.execute(text(f'ALTER SCHEMA "{schema_name}" OWNER TO postgres'))
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema_name}" TO t968rs'))
    # Verify schema creation
    inspector = inspect(engine)
    if schema_name not in inspector.get_schema_names():
        print(f'Schemas: {inspector.get_schema_names()}')
        raise ValueError(f"Schema {schema_name} was not successfully created.")

    return schema_name


def get_p_count(engine, table_name):
    with engine.connect() as conn:
        query = f'SELECT COUNT(*) FROM "{table_name}";'
        result = conn.execute(text(query)).scalar()
        return result

def check_p_table_exists(engine, table_name):
    if isinstance(engine, str):
        engine = get_postgis_engine(engine)

    if inspect(engine).has_table(table_name, schema='public'):
        return True
    else:
        return False


def get_list_of_records(engine, table_name, field_name):
    if isinstance(engine, str):
        engine = get_postgis_engine(engine)

    query = text(f'''SELECT '{field_name}' FROM "{table_name}"''')
    with engine.connect() as conn:
        result = conn.execute(query)
    return [row[0] for row in result.fetchall()]


def get_p_table_columns(engine, table_name):
    if isinstance(engine, str):
        engine = get_postgis_engine(engine)\

    # Ensure the table name is properly quoted
    if "." in table_name:
        schema_name, base_table_name = table_name.split(".")
        qualified_table_name = f'"{schema_name}"."{base_table_name}"'
    else:
        qualified_table_name = f'"{table_name}"'

    logger.info(f"Getting columns for table: {qualified_table_name}")

    with engine.connect() as conn:
        query = text(f'SELECT * FROM {qualified_table_name} LIMIT 0;')
        result = conn.execute(query)
        return result.keys()

def del_p_table(engine, table_name):
    if isinstance(engine, str):
        engine = get_postgis_engine(engine)
    with engine.connect() as conn:
        query = text(f'DROP TABLE "{table_name}";')
        conn.execute(query)


def get_matching_tables(engine, schema_name, pattern=None):
    if isinstance(engine, str):
        engine = get_postgis_engine(engine)

    # Modify query based on whether a pattern is provided
    if pattern is None:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_type = 'BASE TABLE';
        """)
        params = {"schema_name": schema_name}
    else:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_name LIKE :pattern
              AND table_type = 'BASE TABLE';
        """)
        params = {"schema_name": schema_name, "pattern": pattern}

    # Execute query and fetch results
    with engine.connect() as conn:
        result = conn.execute(query, params)
        tables = [row[0] for row in result]

    return tables

def post_count_query(table_name):
    return text(f'SELECT COUNT(*) FROM "{table_name}";')


def check_rebuild_spatial_index(engine, table_name):
    with engine.connect() as conn:
        check_query = spatial_index_queries(table_name, qtype='CHECK')
        result = conn.execute(check_query).fetchone()
        if not result:
            create_query = spatial_index_queries(table_name, qtype='CREATE')

            conn.execute(create_query)
        else:
            logger.info(f"Rebuilding spatial index for '{table_name}'.")
            drop_query = spatial_index_queries(table_name, qtype='DROP')
            conn.execute(drop_query)
            create_query = spatial_index_queries(table_name, qtype='CREATE')
            conn.execute(create_query)


def get_p_geometry_types(engine, table_name):
    # Check if the geometry column has multiple types using TABLESAMPLE
    with engine.connect() as connect:
        geom_type_check_query = f"""
        SELECT DISTINCT GeometryType(geometry) AS geom_type
        FROM {table_name}
        TABLESAMPLE SYSTEM (1)  -- 1% of the table; adjust percentage as needed
        WHERE geometry IS NOT NULL;
        """
        return [row[0] for row in connect.execute(geom_type_check_query).fetchall()]


def add_p_column(engine, table_name, column):
    # Get the column name and type using SQLAlchemy
    column_name = column.name
    column_type = column.type.compile(dialect=engine.dialect)  # Converts to PostgreSQL type string

    # Initialize the query string with basic column addition
    query_string = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'

    # Add NOT NULL clause if the column is not nullable
    if column.nullable is False:
        query_string += ' NOT NULL'

    # Add DEFAULT clause if there is a default value
    if column.default is not None:
        default_value = column.default.arg
        query_string += f' DEFAULT {repr(default_value)}'

    # End the query with a semicolon
    query_string += ';'

    # Execute the query
    with engine.connect() as conn:
        conn.execute(text(query_string))


def get_p_column_differences(existing_columns, columns_template: dict) -> Tuple[List[str], Dict[str, Tuple]]:
    logger.info(f"Existing Columns: {existing_columns}")
    logger.info(f"Columns Template: {columns_template}")

    # Get missing columns
    existing_names = [c.name for c in existing_columns]
    missing_columns = [c for c in columns_template.keys() if c not in existing_names]

    # Get type mismatches
    type_mismatches = {}
    for c in columns_template:
        if c != 'geometry' and c in existing_columns:
            if str(existing_columns[c]) != str(c.type):
                type_mismatches[c] = (existing_columns[c], columns_template[c])

    if missing_columns:
        logger.info(f"Missing Columns: {missing_columns}")
    else:
        logger.info("No missing columns.")
    if type_mismatches:
        for mm, info in type_mismatches.items():
            logger.info(f"\tType Mismatch: {mm} {info}")
    else:
        logger.info("No type mismatches.")
    return missing_columns, type_mismatches


def change_p_column_type(engine, table_name, column_name, new_type):
    with engine.connect() as conn:
        query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};"
        conn.execute(text(query))


def get_p_Columns(engine, table_name):
    columns = []
    with engine.connect() as conn:
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """
        result = conn.execute(text(query)).fetchall()

        # Create SQLAlchemy Column objects
        for row in result:
            column_name, data_type = row
            sqlalchemy_type = map_data_type(data_type)
            if column_name.lower() == 'geometry':
                logger.info(f"Geometry Column: {column_name} {sqlalchemy_type}")
            column = Column(column_name, sqlalchemy_type)
            columns.append(column)

    return columns


def map_pddtype_to_sql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Double
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif dtype == 'geometry':
        return Geometry
    else:
        return String


def map_data_type(data_type):
    if data_type == 'integer':
        return Integer
    elif data_type == 'double precision':
        return Float
    elif data_type == 'boolean':
        return Boolean
    elif data_type in ['USER-DEFINED', 'geometry', 'Geometry']:  # Assuming this might be Geometry
        return Geometry
    else:
        return String  # Default to String for unhandled types


def df_columns_to_sql_columns(df, geom_type='GEOMETRY'):
    if isinstance(geom_type, str):
        geom_type = Geometry(geom_type.upper(), srid=df.crs.to_epsg())

    sql_columns = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'geometry':
            sql_columns.append(Column('geom', geom_type))
        else:
            try:
                postgres_type = map_data_type(dtype)
            except:
                logger.error(f"Failed to map dtype: {dtype}")
                raise ValueError(f"Failed to map dtype: {dtype}")
            sql_columns.append(Column(column_name, postgres_type))
    return sql_columns


def get_postgis_engine(database, user=None, pool_size=20, max_overflow=0):
    """
    Creates a SQLAlchemy engine for connecting to PostGIS with connection pooling.

    Parameters:
    - user (str): Database username.
    - password (str): Database password.
    - host (str): Database host address.
    - port (int): Database port number.
    - database (str): Database name.
    - pool_size (int): Number of connections to keep in the pool.
    - max_overflow (int): Number of connections to allow beyond pool_size.

    Returns:
    - engine (SQLAlchemy Engine): SQLAlchemy engine instance.
    """
    if user is None:
        user = "t968rs"

    logger.info(f"Creating PostGIS engine for database '{database}' with user '{user}'.")

    url = get_sql_url(database, user)
    engine = create_engine(url, pool_size=pool_size, max_overflow=max_overflow)
    return engine


def get_sql_url(database, user):
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password="pU8Mtz12!@",
        host="localhost",
        port=5432,
        database=database
    )
    return url
