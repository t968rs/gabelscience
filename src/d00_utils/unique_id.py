import sys
import geopandas as gpd
import os


class CountID:
    def __init__(self, gdf: gpd.GeoDataFrame,
                 start_value: int = 0,
                 str_len: int = 10,
                 field_name: str = None):

        self.start = start_value
        self.str_len = str_len
        self.field_name = field_name
        if not self.field_name:
            self.field_name = "unique_id"

        self.used_id = set()
        self.count = 0
        if isinstance(gdf, gpd.GeoDataFrame):
            self.class_gdf = gdf

        self._check_string_length()

    def _check_string_length(self):
        feat_count = len(self.class_gdf)
        string_number = ""
        for i in range(self.str_len):
            string_number += "9"
        string_number = int(string_number)
        if feat_count > string_number:
            min_len = len(str(feat_count))
            raise ValueError(f"\nTable has more features ({feat_count:,}) than the string length allows: "
                             f"\n   Max features for {self.str_len} digit ID: {string_number:,}"
                             f"\n   Increase to at least {min_len}")

    def _add_count(self):
        if not self.count or self.count == 0:
            if len(self.used_id) == 0:
                self.count = self.start if self.start else 0
            else:
                self.count = max([int(i) for i in self.used_id]) + 1
        else:
            self.count += 1
        return self.count

    def get_id_as_string(self):
        if isinstance(self.count, int):
            s = str(self.count)
            if len(s) > self.str_len:
                s = s[:self.str_len]
            else:
                zero_adds = self.str_len - len(s) - 1
                z = "0"
                for zero in range(zero_adds):
                    z = z + "0"
                s = z + s
            return s
        else:
            raise ValueError("Count must be an integer")

    def _check_id_in_use(self, id_value):
        if id_value not in self.used_id:
            self.used_id.add(id_value)
        else:
            while id_value in self.used_id:
                self._add_count()
                id_value = self.get_id_as_string()
                if id_value not in self.used_id:
                    self.used_id.add(id_value)
                    break
        return id_value

    def generate_unique_id(self):
        id_dict = {}

        for i, row in self.class_gdf.iterrows():
            self._add_count()
            uid = self.get_id_as_string()
            uid = self._check_id_in_use(uid)
            # print(f"UID: {uid}")
            id_dict[i] = uid
        self.class_gdf[self.field_name] = id_dict

    def final_uniqueness_check(self):
        duplicated = self.class_gdf.duplicated(subset=self.field_name)
        duplicated = duplicated.loc[duplicated == True]
        if len(duplicated) == 0:
            print(f"Unique ID field '{self.field_name}' is unique")
            print(f"  EG: {list(self.used_id)[:5]}")
        else:
            print(f"DUPLICATED: {duplicated}")
            print(f"WARNING: Unique ID field '{self.field_name}' is not unique")


def create_all_unique_id(gdf: gpd.GeoDataFrame, start_value=None, str_len=10, field_name=None):
    init_count = CountID(gdf, start_value, str_len, field_name)
    init_count.generate_unique_id()
    init_count.final_uniqueness_check()


if __name__ == "__main__":
    tables = sys.argv[1]
    starting_value = int(sys.argv[2])
    string_length = int(sys.argv[3])
    fieldname = sys.argv[4]
    kevin = "NO"
    print(f" FC: {tables}\n Start: {starting_value}\n Str Len: {string_length}\n Field: {fieldname}")
    # print(f"Types: {type(tables)}, {type(starting_value)}, {type(string_length)}, {type(fieldname)}")

    if tables and tables != '#':
        table_list = sorted(tables.split(";"))
        for tbl in table_list:
            create_all_unique_id(tbl, starting_value, string_length, fieldname)
