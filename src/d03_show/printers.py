"""Various print functions for different file types"""
import typing as T
from src.d00_utils import table_column_slicer
from src.d00_utils.get_ext_from import get_extension
from rasterio.transform import Affine


def print_attributes(obj, n):
    pattributes = {}
    att_opts = {}
    for i, attribute in enumerate(dir(obj)):
        # print(f"Attribute {i}: {attribute}")
        att_opts[i] = []
        if not attribute.startswith('__'):
            value = getattr(obj, attribute)
            if isinstance(value, float):
                string = str(round(value, n))
            elif isinstance(value, T.Sequence) and not isinstance(value, str):
                string = ', '.join([str(v) for v in value])
            elif "transform" in attribute:
                parts = str(value).split(',')
                print(f'Parts: {parts}')
                line1 = "transform:"
                line2 = f"  | {parts[0]} {parts[1]} {parts[2]} |"
                line3 = f"  | {parts[3]} {parts[4]} {parts[5]} |"
                string = f"{line1}\n{line2}\n{line3}"
            else:
                string = value
            pattributes[i] = f"{attribute}: {string}"

    if len(pattributes) < 3:
        print(', '.join([f"{k}: v" for k, v in pattributes.items()]))
    else:
        if pattributes:
            for i, print_str in pattributes.items():
                print(print_str)
    print("\n")


class PrintFileInfo:
    def __init__(self, file_path, file_type_input=None):
        self.file_path = file_path
        self.file_type_input = file_type_input
        self.file_ext = file_type_input
        self.file_type = get_extension(self.file_ext)
        self.print_director()

    def print_director(self):
        print(f"  Printer found file type {self.file_type} from {self.file_type_input}")
        if self.file_type == ".shp":
            print_shp(self.file_path)


def remove_middle_character(s, max_length=90):
    h = len(s) // 2
    mod = int(round(h - (max_length / 2)))
    # print(f"H: {h}, mod: {mod}")
    return s[:h - mod] + "..." + s[h + mod:]


def print_shp(path):
    import geopandas as gpd
    assert ".shp" in path

    # Read path
    gdf = gpd.read_file(path)
    columns = [c for c in gdf.columns.to_list() if c != "geometry"] + [gdf.index.name]

    # Init first val dict
    first_val_dict = {c: gdf[c][0] for c in gdf.columns}

    column_slices = table_column_slicer.list_slicer(columns, 4)

    print(f" COLUMNS: ---: \n")
    for cs in column_slices:
        print(" || ".join(cs))
    print(f" Example Values by Column --\n")
    for c, v in first_val_dict.items():
        v = str(v)
        if len(v) > 90:
            v = remove_middle_character(v)
        if len(c) >= 8:
            print(f"{c} EG: \t{v}")
        else:
            print(f"{c} EG: \t\t{v}")

    # print by slice


if __name__ == "__main__":
    input_string = "Shapefile"
    input_path = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\Terrain\Index_Lower_Cedar_26915.shp"
    print_shp(input_path)
