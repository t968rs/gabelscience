import geopandas as gpd
import os
from src.specs.raster_specs import create_raster_specs_from_path
from src.d00_utils.bounds_convert import bounds_to_polygon
from src.d03_show.reporting import accept_process_info
from time import time


class CreateRasterIndex:

    def __init__(self, input_folder, results_folder, targ_crs, moverasters):

        self.input_folder = input_folder
        self.output_folder = results_folder

        self.snap_raster = None
        self.tile_dict = {}
        self.tile_bounds = {}
        self.columns = {}
        self.target_crs = targ_crs
        self.moverasters = moverasters
        self.outname_str = self._init_out_naming()

        self.chunks = 32

        self.epsg_list = []
        self.output_dict = {}

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        self.total_size = 0
        self.start_time = time()

    @property
    def bad_exts(self):
        bad_exts = {".img": [".rrd", ".aux", ".xml"],
                    ".tif": [".xml", ".ovr"]}
        return bad_exts

    @property
    def raster_exts(self):
        return [".img", ".tif"]

    @staticmethod
    def get_gdf_from_fc(path):
        # Read Polygon Shapefile
        if ".gdb" in path:
            gdb, fc = os.path.split(path)
            # print(fiona.listlayers(gdb))
            gdf = gpd.read_file(gdb, driver='FileGDB', layer=fc)
        else:
            gdf = gpd.read_file(path)
        c_list = [c for c in gdf.columns.to_list()]
        return gdf, c_list

    def _init_out_naming(self):
        basepath, outfoldername = os.path.split(self.input_folder)
        print(f' Output naming: {outfoldername}')
        return outfoldername

    def find_rasters_match_list(self):

        for root, folders, files in os.walk(self.input_folder):
            for file in files:
                path = os.path.join(root, file)
                self.total_size += os.path.getsize(path)
                parts = list(path.lower().split("."))
                period_parts = ["." + str(p) for p in parts]
                if not set(self.raster_exts).isdisjoint(set(period_parts)):
                    file_ext = list(set(self.raster_exts).intersection(set(period_parts)))[0]
                    # print(f'File ext: {file_ext}')
                    no_append = False
                    if not set(period_parts).isdisjoint(self.bad_exts[file_ext]):
                        no_append = True
                    if not no_append:
                        filename = file.split(file_ext)[0]
                        self.tile_dict[filename] = {"ext": file_ext, "path": path}
        # print(f"Paths: {all_paths}")

        print(f'\nThere are {len(self.tile_dict)} grid tiles\n')

    @staticmethod
    def raster_extents_to_gdf(raster_dict, tgt_crs=None):

        # Get raster extents from raster paths
        # dict[filename] = {"ext": file_ext, "path": path}
        df = {"r_id": [], 'tile': [], 'full_path': [], "ext": [], "folder": [],
              "in_crs": [], 'geometry': []}
        epsg_list = []
        print(f'Finding raster extents ({len(raster_dict)}) and creating GDF polygon...')
        for i, (name, info_dict) in enumerate(raster_dict.items()):
            df['tile'].append(name)
            df['full_path'].append(info_dict['path'])
            folder = os.path.split(info_dict['path'])[0]
            df["folder"].append(folder)
            df['ext'].append(info_dict['ext'])
            df['r_id'].append(i)
            # print(f"Raster Path: {info_dict['path']}")
            specs = create_raster_specs_from_path(info_dict['path'])

            auth_name, epsg_code = specs.crs.to_authority(90)
            epsg_list.append(epsg_code)
            df['in_crs'].append(epsg_code)
            pg, gdf_pg = bounds_to_polygon(*specs.bounds, specs.crs)
            if tgt_crs:
                gdf_pg.to_crs(tgt_crs, inplace=True)
            else:
                gdf_pg = gdf_pg.set_crs(epsg=epsg_code, inplace=True)
            pg = gdf_pg.geometry.values[0]
            # print(f"PG: {pg}")
            df['geometry'].append(pg)

        print(f'Input CRS List: {list(set(epsg_list))}')

        length_list = {}
        for c in df.keys():
            length = len(df[c])
            length_list[c] = length

        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=tgt_crs)
        print(f'GDF: \n{gdf.columns}\n ')
        return gdf, epsg_list

    def move_rasters(self, gdf):
        code_folders = {}
        for code in self.epsg_list:
            folder = os.path.join(self.output_folder, f"EPSG_{code}")
            print(f'Output Folder: {folder}')
            code_folders[code] = folder
            if not os.path.exists(folder):
                os.makedirs(folder)

        # Add outpath column
        if "out_path" not in gdf.columns:
            gdf["out_path"] = None
            gdf['out path'] = gdf["out_path"].astype(str)

        for index, feature in gdf.iterrows():
            # print(feature)
            code = feature['in_crs']
            inpath = feature["full_path"]
            filename = os.path.split(inpath)[1]
            outpath = os.path.join(code_folders[code], filename)
            gdf.at[index, "outpath"] = outpath
            if not os.path.exists(outpath):
                os.rename(inpath, outpath)
        return gdf

    def tile_rasters(self):
        from src.d03_show.printers import PrintFileInfo
        self.find_rasters_match_list()
        tile_gdf, crs_list = self.raster_extents_to_gdf(self.tile_dict, tgt_crs=self.target_crs)
        self.epsg_list = crs_list
        if self.moverasters:
            tile_gdf = self.move_rasters(tile_gdf)

        # Create outputs
        # Test if: target CRS
        outputs = []
        if not self.target_crs:
            for code in self.epsg_list:
                out_shp = os.path.join(self.output_folder, f"Index_{self.outname_str}_{code}.shp")
                if not tile_gdf.crs:
                    tile_gdf = tile_gdf.set_crs(crs=code, inplace=True)
                filtered = tile_gdf[tile_gdf.in_crs == code].to_crs(f"EPSG:{code}")
                filtered.to_file(out_shp, driver='ESRI Shapefile')
                outputs.append(out_shp)
        else:
            out_shp = os.path.join(self.output_folder, f"Index_{self.outname_str}_{self.target_crs}.shp")
            tile_gdf.to_file(out_shp)
            outputs.append(out_shp)
        accept_process_info("create raster tile index", self.total_size, time() - self.start_time)
        PrintFileInfo(outputs[0], "Shapefile")


if __name__ == "__main__":
    root_raster_folder = r"E:\Iowa_2A\01_data\2A_dem\Tiles_meters\processed"
    output_folder = r"E:\Iowa_2A\01_data\2A_dem\Tiles_meters\Topo_Index"
    target_crs = None
    move_files = False

    init_create_tileshp = CreateRasterIndex(root_raster_folder, output_folder, target_crs, move_files)
    init_create_tileshp.tile_rasters()
