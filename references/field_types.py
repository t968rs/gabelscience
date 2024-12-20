from references.fema_field_definitions import FIELD_DEFINITIONS

# Field type conversion dictionary
FIELD_TYPE_CONVERSIONS = {
    "integer": {
        "pandas": ["int64", "Int64", "int32", "Int32", "int16", "Int16", "int8", "Int8"],
        "arcpy": "LONG",
        "postgis": "INTEGER",
    },
    "small_integer": {
        "pandas": ["int16", "Int16"],
        "arcpy": "SHORT",
        "postgis": "SMALLINT",
    },
    "float": {
        "pandas": ["float64", "float32"],
        "arcpy": "FLOAT",
        "postgis": "FLOAT8",
    },
    "double": {
        "pandas": ["float64"],
        "arcpy": "DOUBLE",
        "postgis": "DOUBLE PRECISION",
    },
    "text": {
        "pandas": ["object", "string"],
        "arcpy": "TEXT",
        "postgis": "TEXT",
    },
    "boolean": {
        "pandas": ["bool", "boolean"],
        "arcpy": "SHORT",  # Note: Esri often uses SHORT (0/1) for boolean
        "postgis": "BOOLEAN",
    },
    "date": {
        "pandas": ["datetime64[ns]", "datetime64"],
        "arcpy": "DATE",
        "postgis": "TIMESTAMP",
    },
    "geometry": {
        "pandas": ["geometry"],
        "arcpy": "GEOMETRY",
        "postgis": "GEOMETRY",
    }
}

esri_to_general = {
    "TEXT": "text",
    "FLOAT": "float",
    "DOUBLE": "double",
    "SHORT": "small_integer",
    "LONG": "integer",
    "DATE": "date",
    "BLOB": "text",
    "RASTER": "text",
    "GUID": "text",
    "OID": "integer",
}


def convert_fields_to_postgres_types(input_fema_table):
    from sqlalchemy import String, Integer, Float, Date, DOUBLE_PRECISION

    input_fema_table = input_fema_table.upper()
    print(f"Input Table: {input_fema_table}")
    table_info = FIELD_DEFINITIONS.get(input_fema_table)
    # print(f"Table Info: {table_info}")

    field_names = FIELD_DEFINITIONS.get(input_fema_table).get("field names")
    ftypes = FIELD_DEFINITIONS.get(input_fema_table).get("field types")

    field_types = {
        "Text": String,
        "Double": DOUBLE_PRECISION,
        "Date": Date,
        "Integer": Integer,
    }

    sql_types = {}
    for field in field_names:
        fname_idx = field_names.index(field)
        field_type = ftypes[fname_idx]
        sql_types[field] = field_types[field_type]

    return sql_types
