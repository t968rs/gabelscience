import geopandas as gpd
import os
import shutil
from time import time
from src.d03_show.reporting import accept_process_info

os.environ['GDAL_DATA'] = r'D:\GDAL_STUFF\Library\share\gdal'


class CopyFilesInSHPfield:
    def __init__(self, shp_path, fieldname, out_folder, perf_copy=False):
        self.shp_path = shp_path
        self.fieldname = fieldname
        self.out_folder = out_folder
        self.gdf = None
        self.column_list = None
        self.perf_copy = perf_copy
        self._init_gdf()

    def _init_gdf(self):
        if ".gdb" in self.shp_path:
            gdb, fc = os.path.split(self.shp_path)
            self.gdf = gpd.read_file(gdb, driver='FileGDB', layer=fc)
        else:
            self.gdf = gpd.read_file(self.shp_path)
        self.column_list = [c for c in self.gdf.columns.to_list()]

    def _copy_a_file(self, path):
        if os.path.exists(path):
            filename = os.path.split(path)[1]
            outpath = os.path.join(self.out_folder, filename)
            shutil.copyfile(path, outpath)

    def get_file_list_from_field(self):
        if not os.path.exists(self.out_folder):
            os.makedirs(self.out_folder)

        if not self.fieldname or "full" not in self.fieldname:
            if {"folder", "tile", "ext"}.issubset(self.gdf.columns):
                # Construct the full path using folder, filename, and ext columns
                self.gdf["full_path"] = self.gdf.apply(
                    lambda row: os.path.join(row["folder"], row["tile"] + row["ext"]), axis=1)
            else:
                raise ValueError("The required columns (folder, filename, ext) are not present in the DataFrame")
        unique_paths = list(set(self.gdf["full_path"].unique()))
        print(f"Found: {len(unique_paths)} paths to copy from {self.fieldname} field")

        # Copy the files
        if self.perf_copy:
            copied = []
            sizes = []
            start = time()
            for path in unique_paths:
                sizes.append(os.path.getsize(path))
                self._copy_a_file(path)
                copied.append(path)
            end = time()
            accept_process_info("copy", total_size=sum(sizes),
                                elapased=end - start)

            print(f"Copied {len(copied)} / {len(unique_paths)} files found")


if __name__ == "__main__":
    in_path = r"Z:\Shell_Rock\02_WORKING\Terrain\Shell_Rock_Index_26915.shp"
    field_name = None
    outfolder = r"Z:\Shell_Rock\02_WORKING\Terrain\26915"
    actually_copy = True

    tile_mover = CopyFilesInSHPfield(in_path, field_name, outfolder, actually_copy)
    tile_mover.get_file_list_from_field()
