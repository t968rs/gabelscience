import geopandas as gpd
import os
import shutil
from time import time
from src.d03_show.reporting import accept_process_info


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
        unique_paths = list(set(self.gdf[self.fieldname].unique()))
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
    in_path = r"E:\Iowa_3B\02_mapping\Little_Sioux\Terrain\Index_tiles_littlesioux_26915_2.shp"
    field_name = "full_path"
    outfolder = r"E:\Iowa_3B\02_mapping\Little_Sioux\Terrain\26915"
    actually_copy = True

    tile_mover = CopyFilesInSHPfield(in_path, field_name, outfolder, actually_copy)
    tile_mover.get_file_list_from_field()
