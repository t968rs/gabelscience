from dataclasses import dataclass
from references.fema_field_definitions import FIELD_DEFINITIONS
from references.field_types import FIELD_TYPE_CONVERSIONS, esri_to_general

FEMA_GDB_NULLS = {"Double": -9999, "Date": 8 / 8 / 8888, "String": None}
FEMA_SHP_NULLS = {"Double": -9999, "Date": 8 / 8 / 8888, "String": ""}
OTHER_REQUIRED_FIELDS = ["OBJECTID", "Geometry", "OID", "Shape", "Shape_Length", "Shape_Area",
                         "SHAPE_Area", "SHAPE_Length"]
SPECIAL_FIELDS = {"START_ID": "NP"}

fema_fields = FIELD_DEFINITIONS

@dataclass
class FEMATable:
    table_type: str
    field_names: list
    unique_id: str
    fc_type: str
    field_types: dict
    name: str = None

    @classmethod
    def from_table_type(cls, table_type):
        table_type = table_type.upper()
        if table_type not in FIELD_DEFINITIONS.keys():
            raise ValueError(f"Table type '{table_type}' not found in FIELD_DEFINITIONS.")
        field_names = FIELD_DEFINITIONS.get(table_type).get("field names")
        unique_id = FIELD_DEFINITIONS.get(table_type).get("unique_id")
        fc_type = FIELD_DEFINITIONS.get(table_type).get("fc_type")

        field_types = cls._get_field_types(field_names, table_type)
        name = table_type
        return cls(table_type, field_names, unique_id, fc_type, field_types, name)

    def __post_init__(self):
        if self.table_type not in FIELD_DEFINITIONS.keys():
            raise ValueError(f"Table type '{self.table_type}' not found in FIELD_DEFINITIONS.")
        if len(self.field_names) != len(self.field_types):
            raise ValueError(f"Field names and field types must be the same length.")

    def __str__(self):
        return f"FEMATable: {self.table_type}"

    def __repr__(self):
        output_format = f" -- {self.table_type}"
        for k, v in self.field_types.items():
            output_format += f"\n\t{k}: {v}"
        return output_format

    def as_dict(self):
        ordered_keys = self.__dir__()
        return {key: getattr(self, key) for key in ordered_keys}

    def update_field_types(self, field_types):
        self.field_types = field_types

    def update_a_field_type(self, field_name, field_type):
        self.field_types[field_name] = field_type

    @staticmethod
    def _get_field_types(field_names, table_type):
        field_types = {}
        for field, ftype in zip(field_names, FIELD_DEFINITIONS.get(table_type).get("field types")):
            field_types[field] = ftype
        return field_types

    def to_geopandas(self):
        geopandas_fields = {}
        for field, ftype in self.field_types.items():
            new_type = FieldType.from_general_type(ftype).to_pandas()
            geopandas_fields[field] = new_type
        if 'OBJECTID' not in geopandas_fields:
            geopandas_fields['OBJECTID'] = 'int'
        return geopandas_fields

    @staticmethod
    def d_zone(return_period):

        if return_period == '1%':
            fld_zone = 'A'
            zone_subty = None
        elif return_period == '0.2%':
            fld_zone = 'X'
            zone_subty = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
        else:
            fld_zone, zone_subty = None, None

        return fld_zone, zone_subty


class FieldType:
    general_type: str

    def __post_init__(self):
        if self.general_type not in FIELD_TYPE_CONVERSIONS.keys():
            raise ValueError(f"General type '{self.general_type}' not found in FIELD_TYPE_CONVERSIONS.")

    @classmethod
    def from_general_type(cls, general_type):
        cls.general_type = general_type
        return cls

    @classmethod
    def from_esri(cls, esri_type):
        if esri_type.upper() in esri_to_general:
            cls.general_type = esri_to_general.get(esri_type.upper())
            return cls

    @classmethod
    def to_pandas(cls):
        for gtype, platform in FIELD_TYPE_CONVERSIONS.items():
            if gtype == cls.general_type:
                pandas_type = platform.get("pandas")[0]
                return pandas_type

    @classmethod
    def to_postgis(cls):
        for gtype, platform in FIELD_TYPE_CONVERSIONS.items():
            if gtype == cls.general_type:
                postgis_type = platform.get("postgis")
                return postgis_type

    @classmethod
    def to_arcpy(cls):
        for gtype, platform in FIELD_TYPE_CONVERSIONS.items():
            if gtype == cls.general_type:
                arcpy_type = platform.get("arcpy")
                return arcpy_type


