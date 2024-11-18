# src/d01_processing/postgis_write.py
from tqdm import tqdm
from src.d00_utils.postgis_helpers import *
from src.d00_utils.postgis_helpers import get_postgis_engine, get_matching_tables

logger = getalogger("postgis_merge")

class MergeTables:
    def __init__(self, tgt_tbl, db_name, temp_schema):
        self.target_table = tgt_tbl.strip()
        # if "." in self.target_table:
        #     tgt_schema, tgt_name = self.target_table.split(".")
        #     self.target_table = f'"{tgt_schema}"."{tgt_name}"'

        self.db_name = db_name
        self.temp_schema = temp_schema

        self.engine = get_postgis_engine(self.db_name)

    def check_geometry_registered(self):
        """
        Check if the geometry column is registered in geometry_columns for the target table.
        """
        # Extract schema and table name
        if "." in self.target_table:
            schema_name, table_name = self.target_table.replace('"', '').split(".")
        else:
            schema_name = "public"  # Default schema
            table_name = self.target_table.strip().replace('"', '')

        logger.info(f"Checking geometry registration for table: {schema_name}.{table_name}")

        with self.engine.connect() as conn:
            query = text("""
                SELECT *
                FROM geometry_columns
                WHERE f_table_schema = :schema_name
                  AND f_table_name = :table_name
            """)
            result = conn.execute(query, {"schema_name": schema_name, "table_name": table_name})

            # Fetch the first row of the result
            fetched = result.fetchone()
            logger.info(f"Fetched Row: {fetched}")

            if fetched:
                column_names = result.keys()
                return dict(zip(column_names, fetched))

        logger.error("Geometry column not registered in target table.")
        return None

    def append_and_replace(self, columns_to_set, temp_table_name, geom_name):
        # Extract schema and table name for temp table
        temp_table_name = temp_table_name.replace('"', '')
        if "." in temp_table_name:
            temp_schema, temp_table = temp_table_name.split(".")
        else:
            temp_schema = "public"
            temp_table = temp_table_name

        # Extract schema and table name for target table
        tgt_tbl_name = self.target_table.replace('"', '')
        if "." in tgt_tbl_name:
            target_schema, target_table_name = tgt_tbl_name.split(".")
        else:
            target_schema = "public"
            target_table_name = tgt_tbl_name

        # Detect column types for the target table
        with self.engine.connect() as conn:
            column_type_query = text(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema
                  AND table_name = :table
            """)
            column_types = {
                row[0]: row[1]
                for row in conn.execute(column_type_query, {"schema": target_schema, "table": target_table_name})
            }

        # Generate the SET clause for the UPDATE operation with type casting
        update_set_clause = ", ".join([
            f'"{col}" = "temp"."{col}"::{column_types[col]}' if col in column_types else f'"{col}" = "temp"."{col}"'
            for col in columns_to_set
        ])

        # Generate the column and value lists for the INSERT operation with type casting
        insert_columns = f'"{geom_name}", ' + ", ".join([f'"{col}"' for col in columns_to_set])
        insert_values = f'"temp"."{geom_name}", ' + ", ".join([
            f'"temp"."{col}"::{column_types[col]}' if col in column_types else f'"temp"."{col}"'
            for col in columns_to_set
        ])

        # Perform the update and insert operations in PostGIS
        with self.engine.connect() as conn:
            # Update existing records with matching geometries
            update_query = text(f"""
                UPDATE "{target_schema}"."{target_table_name}" AS "tgt"
                SET {update_set_clause}
                FROM "{temp_schema}"."{temp_table}" AS "temp"
                WHERE ST_Equals("tgt"."{geom_name}", "temp"."{geom_name}")
            """)
            conn.execute(update_query)

            # Insert new records that don’t have matching geometries
            insert_query = text(f"""
                INSERT INTO "{target_schema}"."{target_table_name}" ({insert_columns})
                SELECT {insert_values}
                FROM "{temp_schema}"."{temp_table}" AS "temp"
                WHERE NOT EXISTS (
                    SELECT 1 FROM "{target_schema}"."{target_table_name}" AS "tgt"
                    WHERE ST_Equals("tgt"."{geom_name}", "temp"."{geom_name}")
                )
            """)
            conn.execute(insert_query)

        # Drop the temporary table
        with self.engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{temp_schema}"."{temp_table}"'))

    def merge_temp_tables_into_target(self):
        """
        Merge all temp tables into the target table.
        """

        # Get all src tables
        temp_tables = get_matching_tables(self.engine, self.temp_schema)
        if not temp_tables:
            logger.warning(f"No tables found in schema {self.temp_schema}.")
            return
        temp_tables = [f'{self.temp_schema}.{table}' for table in temp_tables]
        logger.info(f"Temp Tables: {temp_tables}")

        # Get all columns from the source tables
        src_columns = set()
        for table in temp_tables:
            src_columns.update(get_p_table_columns(self.engine, table))

        logger.info(f"Source Columns: {src_columns}")

        # Dynamically build the SET clause for the SQL UPDATE query
        geom_registered = self.check_geometry_registered()
        if not geom_registered:
            logger.error("Geometry column not registered in target table.")
            return

        # Get the columns that are common to both the source and target tables
        target_columns = set(get_p_table_columns(self.engine, self.target_table))
        columns_to_set = target_columns.intersection(src_columns)
        columns_to_set = [col for col in columns_to_set if col != geom_registered['f_geometry_column']]

        pbar = tqdm(total=len(temp_tables), desc="Merging Temp Tables")
        for temp_table in temp_tables:
            self.append_and_replace(columns_to_set, temp_table, geom_registered['f_geometry_column'])
            pbar.update(1)

        # Drop the temporary schema (this will also remove temp_table)
        with self.engine.connect() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.temp_schema}" CASCADE'))



if __name__ == "__main__":
    # Set up the PostGIS connection
    database_name = "nfhl_tr32"
    source_schema = "S_FLD_HAZ_AR_temp"
    target_table = "public.S_FLD_HAZ_AR"

    # Create the MergeTables object
    merger = MergeTables(target_table, database_name, source_schema)

    # Merge the temp tables into the target table
    merger.merge_temp_tables_into_target()
# f'"{schema_name}"."{table_name}"'