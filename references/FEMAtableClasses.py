from sqlalchemy import String, Integer, Float, Date, DOUBLE_PRECISION, Column
from fema_field_definitions import FIELD_DEFINITIONS
from references.field_types import convert_fields_to_postgres_types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.d00_utils.postgis_helpers import get_postgis_engine



engine = get_postgis_engine("nfhl_tr32")
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class S_FLD_HAZ_AR(Base):
    __table_name__ = "S_FLD_HAZ_AR"
    __table_args__ = {"schema": "public"}
    ogc_fid = Column(Integer, primary_key=True)
    dfirm_id = Column(String)
    fld_ar_id = Column(String)
    fld_zone = Column(String)


class FEMATable:
    def __init__(self, table_type):
        self.table_type = table_type
        self.field_names = FIELD_DEFINITIONS.get(table_type).get("field names")
        self.unique_id = FIELD_DEFINITIONS.get(table_type).get("unique_id")
        self.fc_type = FIELD_DEFINITIONS.get(table_type).get("fc_type")
        self.field_types = FIELD_DEFINITIONS.get(table_type).get("field types")
        self.sql_types = convert_fields_to_postgres_types(self)

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

    def get_sql_types(self):
        return self.sql_types

    def get_field_types(self):
        return self.field_types

    def get_field_names(self):
        return self.field_names

    def get_unique_id(self):
        return self.unique_id

    def get_fc_type(self):
        return self.fc_type

    def get_table_type(self):
        return self.table_type

    def get_field_type(self, field_name):
        return self.field_types[field_name]

    def get_sql_type(self, field_name):
        return self.sql_types[field_name]