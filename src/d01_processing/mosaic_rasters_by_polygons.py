import shutil
import geopandas as gpd
import fiona
from shapely.geometry import box
from shapely.geometry import Polygon
from osgeo import gdal
import rasterio
import rasterio.merge
import rasterio.windows
import rasterio.warp
from rasterio.enums import Resampling
from affine import Affine
import os
import threading
import xarray as xr
import rioxarray
from rioxarray.merge import merge_arrays
import geocube.vector



class MosaicRetiler:

    def __init__(self, input_folder, output_folder, tile_fc, ext, tgt_cellsize):

        self.input_folder = input_folder
        self.tile_fc = tile_fc
        self.ext = ext
        self.output_folder = output_folder
        self.tgt_cellsize = tgt_cellsize

        self.snap_raster = None
        self.tile_dict = {}
        self.tile_bounds = {}
        self.columns = {}
        self.target_crs = None

        self.chunks = 32

        self.raster_grid_dictionary = {}
        self.output_dict = {}
        self.merged_bounds = ()

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        self.intmd_folder = os.path.join(self.output_folder, "intmd")
        if not os.path.exists(self.intmd_folder):
            os.makedirs(self.intmd_folder)

    @property
    def bad_exts(self):
        bad_exts = {".img": [".rrd", ".aux", ".xml"],
                    ".tif": [".xml", ".ovr"]}
        return bad_exts

    @staticmethod
    def print_raster(raster):
        print(
            f"shape: {raster.rio.shape}\n"
            f"resolution: {raster.rio.resolution()}\n"
            f"bounds: {raster.rio.bounds()}\n"
            f"CRS: {raster.rio.crs}\n"
            f"Chunks: {raster.chunksizes}\n"
            f"Size: {round(raster.nbytes / (1024 ** 3), 2)} GB\n")

    @staticmethod
    def bounds_to_polygon(xmin, ymin, xmax, ymax, crs):

        # Create GDF polygon from bounding box
        bbox = box(xmin, ymin, xmax, ymax)

        pg = Polygon(bbox)

        return gpd.GeoDataFrame(index=[0], geometry=[pg], crs=crs)

    @staticmethod
    def create_mosaic_extent(rasterlist, area):

        leftlist = []
        bottomlist = []
        rightlist = []
        toplist = []
        crs = None
        for path in rasterlist:
            # print(f'Raster Name: {os.path.split(path)[1]}')
            dataset = rasterio.open(path)
            window = rasterio.windows.get_data_window(dataset.read(1))
            if not crs:
                crs = dataset.crs
            bounds = rasterio.windows.bounds(window, dataset.transform)
            leftlist.append(bounds[0])
            bottomlist.append(bounds[1])
            rightlist.append(bounds[2])
            toplist.append(bounds[3])
        max_bounds = (min(leftlist), min(bottomlist), max(rightlist), max(toplist))
        print(f'Looked through {len(rasterlist)} rasters for spatial extents')
        print(f'Max Bounds: {max_bounds}\n')

        # export bounds as SHP
        bbox = box(*max_bounds)
        geom = [*bbox.exterior.coords]
        # print(geom)
        geom = Polygon(geom)
        print(geom)
        gdf = gpd.GeoDataFrame(index=[0], geometry=[geom], crs=crs)
        box_path = os.path.join(r"A:\Iowa_1A\02_mapping\Grids_Turkey", f"bounds_{area}.shp")
        if os.path.exists(box_path):
            base, name = os.path.split(box_path)
            if "." in name:
                name = name.split(".")[0]
            for obj in os.listdir(base):
                if name in obj:
                    try:
                        os.remove(obj)
                    except FileNotFoundError:
                        pass
        gdf.to_file(box_path)

        return max_bounds

    @staticmethod
    def is_it_integer(n):
        try:
            float(n)
        except ValueError:
            return False
        else:
            return int(float(n))

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

    def create_gridded_subsets(self):

        grid_gdf, c_list = self.get_gdf_from_fc(path=self.tile_fc)
        self.target_crs = grid_gdf.crs
        print(f"--Columns: \n{c_list}")
        id_col = [c for c in c_list if "id_" in c][0]
        tilelist_col = [c for c in c_list if "InputT" in c][0]
        self.columns["id"] = id_col
        self.columns['tile list'] = tilelist_col
        print(f' ID Col: {id_col}')
        for index, feature in grid_gdf.iterrows():
            # print(feature)
            grid_id = feature[f"{id_col}"]
            tile_list = list(feature[f"{tilelist_col}"].split(","))
            # print(tile_list)
            self.tile_dict[grid_id] = list(set(tile_list))
            self.tile_bounds[grid_id] = feature.geometry.bounds
            # print(self.tile_bounds)

    def find_rasters_match_list(self):

        tile_path_dict = {}
        all_paths = []
        for root, folders, files in os.walk(self.input_folder):
            for file in files:
                path = os.path.join(root, file)
                if self.ext in file.lower():
                    no_append = False
                    for ext in self.bad_exts[self.ext]:
                        if ext in path.lower():
                            no_append = True
                            break
                    if not no_append:
                        all_paths.append(path)
        # print(f"Paths: {all_paths}")

        found_count = 0
        for grid_id, input_tiles in self.tile_dict.copy().items():
            tile_path_dict[grid_id] = []
            for path in all_paths:
                for tile in input_tiles:
                    if tile in path:
                        found_count += 1
                        tile_path_dict[grid_id].append(path)
                        break
        self.tile_dict = tile_path_dict

        for k, v in self.tile_dict.copy().items():
            if v is None or len(v) == 0:
                # print(f'  No rasters found for tile {k}')
                self.tile_dict.pop(k)

        for k, v in self.tile_dict.copy().items():
            print(f"--Area {k}: \n {v}")
            self.prune_non_overlapping_tiles(area=k)

        print(f'\nThere are {len(self.tile_dict)} grid tiles\nFound {found_count} paths to go with them.')

    @staticmethod
    def get_array_resolution(raster):
        array = rasterio.open(raster)
        trans = array.transform
        crs = array.crs
        signx, signy = trans[0] / abs(trans[0]), trans[4] / abs(trans[4])
        height, width = array.shape
        hw = (height, width)
        bounds = array.bounds
        # print(f'\n--Affine: \n{trans}')
        target_res = (array.res[0] * signx, array.res[1] * signy)
        # print(f'Resolution: {target_res}\n')
        array.close()

        # Get ABS Resolution
        abs_res = []
        for dim in list(target_res):
            abs_res.append(abs(dim))
        abs_res = tuple(abs_res)

        return target_res, abs_res, crs, trans, hw, bounds

    @staticmethod
    def get_transform(src_array, dst_array):

        print(f' Finding overlap window...')
        src_window = rasterio.windows.get_data_window(src_array.read(1))
        # dst_window = rasterio.windows.get_data_window(dst_array.read(1))
        print(f'   Source window: {src_window}')
        # print(f'   Dest window: {src_window}')
        # overlap_window = rasterio.windows.intersection(src_window, dst_window)
        overlap_window = rasterio.windows.round_window_to_full_blocks(src_window, dst_array.block_shapes)
        print(f'   Overlap window: {overlap_window}')

        transform, width, height = rasterio.warp.calculate_default_transform(src_array.crs, dst_array.crs,
                                                                             src_array.width,
                                                                             src_array.height,
                                                                             src_array.bounds[0], src_array.bounds[1],
                                                                             src_array.bounds[2], src_array.bounds[3],
                                                                             dst_width=overlap_window.width,
                                                                             dst_height=overlap_window.height)
        return transform, (width, height)

    def project_one(self, raster, outfile):
        if os.path.exists(outfile):
            return

        #snapcell_size, snapabs_size, snapcrs, snaptransform, snaphw, snap_bounds = self.get_array_resolution(
            #self.snap_raster)
        # print(f"Snap Transform:--\n {snaptransform}")
        cell_size, abs_size, crs, transform, hw, bounds = self.get_array_resolution(raster)

        # print(f'Will project to {snapcrs} at {snapcell_size}')

        array = rioxarray.open_rasterio(raster, chunks=self.chunks, lock=False)
        with rasterio.Env(CHECK_WITH_INVERT_PROJ=True):
            target_bounds = array.rio.transform_bounds(dst_crs=self.target_crs, densify_pts=500, recalc=True)
            print(f'Target Bounds: \n{target_bounds}')
            man_transform = Affine(self.tgt_cellsize, 0.0, target_bounds[0],
                                   0.0, -self.tgt_cellsize, target_bounds[3],
                                   0.0, 0.0, 1.0)
            # print(f"Manual transform: \n{man_transform}\n")
            t_h = int(round(((target_bounds[2] - target_bounds[0]) / self.tgt_cellsize) + 0.5))
            t_w = int(round(((target_bounds[3] - target_bounds[1]) / self.tgt_cellsize) + 0.5))
            # print(f'Destination HW: {t_h}, {t_w}')
            target_transform, width, height = rasterio.warp.calculate_default_transform(src_crs=crs, dst_crs=self.target_crs,
                                                                                        width=hw[1], height=hw[0],
                                                                                        left=bounds[0],
                                                                                        bottom=bounds[1],
                                                                                        right=bounds[2], top=bounds[3],
                                                                                        dst_width=t_w, dst_height=t_h)
            # print(f'Rasterio transform: {target_transform}\n {width}, {height}')
            target_transform = Affine(target_transform[0], target_transform[1], target_transform[2],
                                      target_transform[3], target_transform[4], target_transform[5],
                                      target_transform[6], target_transform[7], target_transform[8])
        print(f'My transform: {target_transform}')

        print(f'  Reprojecting 1 raster')
        valid_array = array * 0 + 1
        outline_pg = geocube.vector.vectorize(valid_array)
        outline_pg = outline_pg.to_crs(self.target_crs)

        base, file = os.path.split(outfile)
        shp_file = file.replace(".tif", ".shp")
        shp_path = os.path.join(base, f"outline_{shp_file}")
        outline_pg.to_file(shp_path)
        array = array.rio.reproject(dst_crs=self.target_crs, res=self.tgt_cellsize,
                                    resampling=Resampling.bilinear, nodata=-9999)
        array = array.astype('float32')
        array = array.rio.pad_box(*target_bounds, None)
        array = array.rio.interpolate_na()
        array = array.rio.clip(outline_pg.geometry.values, from_disk=True, all_touched=True)

        print(f' Saving grid to disc as {outfile}')
        print(f' Output res {array.rio.resolution()}')
        array.rio.to_raster(outfile, tiled=True, lock=threading.Lock(), compress='LZW')
        array.close()
        print("    ...Saved output.\n\n")

    def mosaic_rasters(self, pathlist, area):
        outpath = os.path.join(self.output_folder, f"DEM_mosaic_{area}.tif")
        self.output_dict[area] = outpath
        # Get profile from first raster
        with rasterio.open(pathlist[0]) as src:
            profile = src.profile
            profile.update(compress="lzw")
            # print(f'\nProfile: ')
            # for k, v in profile.items():
            # print(f'  {k}: {v}')

        print(f'\n...Setting up tile output mosaic {area} functions ({len(pathlist)}) indl rasters...')
        map2array = []
        all_masked = 0
        for raster in pathlist:
            anarray = rioxarray.open_rasterio(raster, chunks=self.chunks, lock=False)
            mask_val = anarray.rio.nodata
            # print(f'Input mask: {mask_val}')
            if int(mask_val) != -9999:
                # tvalue = anarray.where(anarray == -9999)
                anarray.rio.write_nodata(-9999, encoded=True, inplace=True)
            else:
                all_masked += 1
                map2array.append(anarray)
        # Max extent
        if all_masked >= len(pathlist):
            self.merged_bounds = self.create_mosaic_extent(rasterlist=pathlist, area=area)

        # Inputs and outputs
        pathlist_new = []
        for path in pathlist:
            base, infile = os.path.split(path)
            filename = infile.split(".")[0]
            if not os.path.exists(self.intmd_folder):
                os.makedirs(self.intmd_folder)
            intmd_path = os.path.join(self.intmd_folder, filename + ".tif")
            pathlist_new.append(intmd_path)
            if not os.path.exists(intmd_path):
                self.project_one(path, intmd_path)
        self.tile_dict[area] = pathlist_new

        if len(pathlist_new) != 0:
            if not os.path.exists(outpath):
                self.merge_multiple(paths=pathlist_new, outfile=outpath, area=area)
                if self.snap_raster is None:
                    self.snap_raster = outpath
            elif len(pathlist) == 1:
                # print(f'Only one plan: {area}')
                self.project_one(raster=pathlist[0], outfile=outpath)
            else:
                print(f'Exists in output: {area}')

    def prune_non_overlapping_tiles(self, area):
        paths = self.tile_dict[area]
        if len(paths) == 0:
            self.tile_dict.pop(area)
            return

        overallgdf, col_list = self.get_gdf_from_fc(self.tile_fc)
        tile_bounds = self.tile_bounds[area]
        tile_gdf = self.bounds_to_polygon(*tile_bounds, crs=overallgdf.crs)
        newpaths = []
        removed = []
        for path in paths:
            rastername = os.path.split(path)[1]
            array = rioxarray.open_rasterio(path, chunks=self.chunks, lock=False)
            array_crs = array.rio.crs
            array_bounds = array.rio.bounds()
            raster_outline = self.bounds_to_polygon(*array_bounds, array_crs)
            raster_outline = raster_outline.to_crs(overallgdf.crs)
            array.close()
            intersect = tile_gdf.overlay(raster_outline, how='intersection')
            if not intersect.empty:
                print(f'Intersect Areas: {intersect.area}')
                print(f'Intersect Area Values: {intersect.area.values}')
                if len(intersect.area.values) == 1:
                    int_area = float(intersect.area.values[0])
                else:
                    int_area = None
                print(f' Area: {int_area}')
                if int_area is None or int_area < 500:
                    print(f'Area of overlap (Area {area} and {rastername}): {intersect.area.values})')
                    removed.append(path)
                else:
                    newpaths.append(path)
            else:
                removed.append(path)
        print(f' Area {area} Removed {len(removed)} non-overlapping rasters.')

        if len(newpaths) == 0:
            self.tile_dict.pop(area)
        else:
            self.tile_dict[area] = newpaths

    def limit_by_output_tiling(self):
        print("\n")
        # Open as GDF
        if ".gdb" in self.tile_fc:
            gdb, fc = os.path.split(self.tile_fc)
            # print(fiona.listlayers(gdb))
            grid_gdf = gpd.read_file(gdb, driver='FileGDB', layer=fc)
        else:
            grid_gdf = gpd.read_file(self.tile_fc)
        grid_gdf = grid_gdf.explode(index_parts=False)

        # Drop columns
        id_col, tilelist_col = self.columns["id"], self.columns["tile list"]
        keeps = [id_col, tilelist_col, 'geometry']  # 'InputTileList',
        to_drop = [c for c in grid_gdf.columns if c not in keeps]
        grid_gdf = grid_gdf.drop(columns=to_drop)
        print(f"GDF Columns: {grid_gdf.columns}")

        for area, path in self.output_dict.copy().items():
            row = grid_gdf.loc[grid_gdf[id_col] == area]
            tile_id = row[id_col].values[0]
            pg_feature = row.geometry
            row.to_file(os.path.join(self.output_folder, f"test_{area}__{os.path.split(path)[1]}.shp"))
            array = rioxarray.open_rasterio(path, chunks=self.chunks, lock=False)

            clipped = array.rio.clip(pg_feature.values, all_touched=True, from_disk=True)

            # Export to TIF
            base, file = os.path.split(path)
            name = file.replace(".tif", "")
            outpath = os.path.join(base, f"{name}_clipped.tif")
            clipped.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW', windowed=True)
            self.output_dict[area] = outpath
            array.close()

            if os.path.exists(outpath):
                os.remove(path)

    def run_retile_rasters(self):

        self.create_gridded_subsets()
        self.find_rasters_match_list()

        for k, v in self.tile_dict.copy().items():
            print(f'\n---{k}---\n{v}')

            self.mosaic_rasters(v, k)

        self.limit_by_output_tiling()
        print("\n")
        for k, v in self.output_dict.items():
            print(f"--TILE {k}--\n  Mosaic: {v}")


if __name__ == "__main__":
    infolder = r"A:\KS\01_Data\raw_DEM"
    outfolder = r"A:\KS\02_mapping\Terrain_FishnetGrids\dem_testing25"
    tiling_fc = None # r"A:\KS\02_mapping\Terrain_FishnetGrids\Grid_Mosaic_UpperRepublican.shp"
    # snapping_raster = r"A:\KS\02_mapping\Terrain_FishnetGrids\dem_testing\snap_raster.tif"
    extension = ".img"
    target_cell_size = 3.28083333

    initialize = MosaicRetiler(infolder, outfolder, tiling_fc, extension, target_cell_size)
    initialize.run_retile_rasters()
