import os
import threading
import geopandas as gpd
import rasterio
import rasterio.merge
import rasterio.warp
import rasterio.windows
import rioxarray
import xarray as xr
from rasterio.enums import Resampling
from rioxarray.merge import merge_arrays
from shapely.geometry import Polygon, box


class MosaicBy:

    def __init__(self, terrain, input_folder, output_folder, sting_tolookfor, ext, mask, sub_pairing_only):

        self.terrain_file = terrain
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.string_tolookfor = sting_tolookfor
        self.ext = ext
        self.mask = mask
        self.subs_only = sub_pairing_only

        self.chunks = 32
        if self.subs_only:
            self.chunks = 128
        if self.string_tolookfor in [None, '', '#']:
            self.string_tolookfor = None

        self.raster_grid_dictionary = {}
        self.merged_bounds = ()

        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

    @staticmethod
    def print_raster(raster):
        print(
            f"shape: {raster.rio.shape}\n"
            f"resolution: {raster.rio.resolution()}\n"
            f"bounds: {raster.rio.bounds()}\n"
            f"CRS: {raster.rio.crs}\n"
            f"Chunks: {raster.chunksizes}\n"
            f"Size: {round(raster.nbytes / (1024 ** 3), 2)} GB\n")

    def create_raster_grid_list(self, folder):

        raster_grids = {}
        tiff_list = os.walk(folder)
        for root, folders, files in tiff_list:
            for file in files:
                if self.ext in file.lower():

                    if ".xml" not in file.lower() and ".ovr" not in file.lower():
                        path = os.path.join(root, os.path.join(root, file))
                        print(f'Path: {path}')
                        # print(f'Path Test: {path}')
                        # split = file.split('.')
                        # print(f'Split {split}')
                        # arcpy.AddMessage(f'File {file}: {split[1]}')
                        # print(f'Ext: {split[1].lower()}')
                        # print(f'Name: {tifname}')
                        raster_grids[file] = path
        self.raster_grid_dictionary = raster_grids

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

    def create_sub_pairs(self):

        pair_dict = {}

        removals = []
        for rastername, path in self.raster_grid_dictionary.items():
            filename = rastername
            rastername = rastername.replace(".", "_")
            # print(f'Rastername: {rastername}')
            for part in rastername.split("_"):
                if len(part) in [3, 4]:
                    # print(f'PArt: {part}')
                    isnumber = self.is_it_integer(part)
                    if isnumber:
                        # print(f' Number: {isnumber}')
                        removals.append(filename)
                        if isnumber not in pair_dict:
                            pair_dict[isnumber] = [path]
                        else:
                            pair_dict[isnumber].append(path)

        for huc10, filelist in pair_dict.items():
            # print(f'\nHUC10: {huc10}')
            if len(str(huc10)) < 4:
                huc10 = "0" + str(huc10)
            # print(f'Sending {huc10}')
            self.mosaic_rasters(pathlist=filelist, area=str(huc10))

        # remove these rasters from dict
        for removal in removals:
            self.raster_grid_dictionary.pop(removal)

    def create_crs_groups(self):

        crs_groups = {}
        for name, path in self.raster_grid_dictionary.items():
            dataset = rasterio.open(path)
            incrs = dataset.crs
            # print(f'String: {incrs.to_wkt()}')
            if "Iowa North" in incrs.to_wkt():
                outcrs = 3417
            elif "Iowa South" in incrs.to_wkt():
                outcrs = 3418
            else:
                outcrs = None

            if outcrs is not None:
                if outcrs not in crs_groups:
                    crs_groups[outcrs] = [path]
                else:
                    crs_groups[outcrs].append(path)

        for k, v in crs_groups.items():
            print(f'\nCRS {k}: {v}')

        return crs_groups

    def remaining_rasters_dict(self):
        prechunk = True
        print(f'Creating mosaic for "the rest":\n  {self.input_folder}')

        pathlist = []

        for rastername, path in self.raster_grid_dictionary.items():
            pathlist.append(path)

        if len(pathlist) > 2 and prechunk == False:
            chunks = self.divide_chunks(pathlist, n=2)
            for i, chunk in enumerate(chunks):
                self.mosaic_rasters(chunk, area=f"remaining_{i}")
        elif prechunk:
            crs_groups = self.create_crs_groups()
            for each_crs, grouppaths in crs_groups.items():
                self.mosaic_rasters(grouppaths, each_crs)

    @staticmethod
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def get_array_resolution(raster):
        array = rasterio.open(raster)
        trans = array.transform
        crs = array.crs
        signx, signy = trans[0] / abs(trans[0]), trans[4] / abs(trans[4])
        # print(f'\n--Affine: \n{trans}')
        target_res = (array.res[0] * signx, array.res[1] * signy)
        print(f'Resolution: {target_res}\n')
        array.close()

        # Get ABS Resolution
        abs_res = []
        for dim in list(target_res):
            abs_res.append(abs(dim))
        abs_res = tuple(abs_res)

        return target_res, abs_res, crs

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

    def match_target_raster(self, inraster, outpath):
        # Cell sizing
        target_cellsize, abs_cellsize, crs = self.get_array_resolution(self.terrain_file)
        print(f'Target Cell Sizes: {target_cellsize}')
        # anarray = rasterio.open(inraster, chunks=self.chunks, lock=False)
        with rasterio.open(self.terrain_file, chunks=self.chunks, lock=False) as terrain_array:
            target_crs = terrain_array.crs

        # transform, dims = self.get_transform(anarray, terrain_array)
        # print(f'   Specs of overlap window: \n{transform}')

        anarray = rioxarray.open_rasterio(inraster, chunks=self.chunks, lock=False)
        outraster = anarray.rio.reproject(dst_crs=target_crs, resolution=abs_cellsize,
                                          resampling=Resampling.bilinear, nodata=-9999)
        print(f'Old Resolution: {anarray.rio.resolution()}')
        print(f'New Resolution: {outraster.rio.resolution()}')
        outpath = os.path.join(self.output_folder, "test3.tif")
        outraster.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW')
        # outraster = anarray.rio.reproject_match(terrain_array)
        print(f' Aligned first grid with terrain ({inraster})')

        anarray.close()
        return outraster

    def project_one(self, raster, outfile):

        cell_size, abs_size, crs = self.get_array_resolution(self.terrain_file)
        array = rioxarray.open_rasterio(raster, chunks=self.chunks, lock=False)
        if "wse" not in self.terrain_file.lower():
            print(f'  Reprojecting to match terrain res')
            array = array.rio.reproject(dst_crs=crs, resolution=abs_size,
                                        resampling=Resampling.bilinear, nodata=-9999)
        else:
            print(f'  Reprojecting using "match"')
            snapper = rioxarray.open_rasterio(self.terrain_file, chunks=self.chunks, lock=False)
            array = array.rio.reproject_match(snapper, resampling=Resampling.bilinear)
            depth_array = xr.DataArray(array.load()).chunk(chunks={"band": 1, "y": 1024, "x": 1024})
            print(f'\n After alignment, depth raster specs---')
            self.print_raster(depth_array)
            print(f'   Matched coords')
            # outraster = outraster.assign_coords({"x": snapper.x, "y": snapper.y})
            snapper.close()

        print(f' Saving mosaic grid to disc as {outfile}')
        print(f' Output res {array.rio.resolution()}')
        array.rio.to_raster(outfile, tiled=True, lock=threading.Lock(), compress='LZW')
        array.close()
        print("Done. Saved output.\n\n")

        return

    def merge_multiple(self, paths, outfile):
        cell_size, abs_size, crs = self.get_array_resolution(self.terrain_file)
        map2array = []
        for raster in paths:
            anarray = rioxarray.open_rasterio(raster, chunks=self.chunks, lock=False)
            mask_val = anarray.rio.nodata
            if int(mask_val) != -9999:
                print(f'Input mask: {mask_val}')
                anarray.rio.write_nodata(-9999, encoded=True, inplace=True)
            map2array.append(anarray)
        print(f' Created individual arrays. Output cell size: {abs_size}')
        da_merged = rioxarray.merge.merge_arrays(map2array, nodata=-9999, res=abs_size,
                                                 method='max', bounds=self.merged_bounds)  #

        print(f' Merged arrays. Output array resolution: {da_merged.rio.resolution()}')
        print(f' Saving mosaic grid to disc as {outfile}')
        da_merged.rio.to_raster(outfile, tiled=True, lock=threading.Lock(), compress='LZW', windowed=True)

    def mosaic_rasters(self, pathlist, area):
        outpath = os.path.join(self.output_folder, f"{self.string_tolookfor}_mosaic_{area}.tif")
        if os.path.exists(outpath):
            print(f'Exists in output: {area}')
            return
        # Get profile from first raster
        with rasterio.open(pathlist[0]) as src:
            profile = src.profile
            profile.update(compress="lzw")
            print(f'\nProfile: ')
            for k, v in profile.items():
                print(f'  {k}: {v}')
                print(f'  {self.string_tolookfor}')

        print(f'\nSetting up {area} merge function ({len(pathlist)} indl rasters)...')
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

        if len(pathlist) == 1:
            print(f'Only one plan: {area}, {self.string_tolookfor}')
            self.project_one(raster=pathlist[0], outfile=outpath)
        else:
            self.merge_multiple(paths=pathlist, outfile=outpath)

        print(f"Done. Saved output: {outpath}\n\n")

    def run_mosaic_rasters(self):

        self.create_raster_grid_list(folder=self.input_folder)
        if self.subs_only:
            self.create_sub_pairs()
        else:
            self.remaining_rasters_dict()


if __name__ == "__main__":
    for return_string in ["0_2pct", "01plus", "02pct", "04pct", "10pct"]:  # "01pct",
        # "0_2pct", "01pct", "01plus", "02pct", "04pct", "10pct"

        terrain_path = r"A:\Iowa_3B\02_mapping\Grids_LowerBigSioux_01\Terrain\terrain_DEM_vft.tif"
        # return_string = "0_2pct"
        infolder = rf"A:\Iowa_3B\01_data\10170203_Sioux\repo\{return_string}"
        outfolder = rf"A:\Iowa_3B\02_mapping\Grids_LowerBigSioux_01\01_RAW_Mosaic\{return_string}"
        limiting_string = f"wse_{return_string}"
        extension = ".tif"
        sub_pairs = True

        mask_raster = r''

        initialize = MosaicBy(terrain_path, infolder, outfolder, sting_tolookfor=limiting_string,
                              ext=extension, mask=mask_raster, sub_pairing_only=sub_pairs)
        initialize.run_mosaic_rasters()
