import geopandas as gpd
import os
import shutil


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
            for path in unique_paths:
                self._copy_a_file(path)
                copied.append(path)
            print(f"Copied {len(copied)} / {len(unique_paths)} files found")


in_path = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\Terrain\Input_Terrain_Index_Lower_Cedar.shp"
field_name = "full_path"
outfolder = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\Terrain\Lower_Cedar_26915"
actually_copy = True

tile_mover = CopyFilesInSHPfield(in_path, field_name, outfolder, actually_copy)
tile_mover.get_file_list_from_field()
