

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