import contextlib
import os
import sys
import threading
from time import sleep
from time import time

import dask.config
import rasterio
import rasterio.warp
import rasterio.windows
import rioxarray as rioxr
import xarray as xr
from dask.distributed import Client, LocalCluster
from rasterio.crs import CRS
from tqdm import tqdm
from src.d01_processing.raster_ops import mask_with_ones
from src.d00_utils import maths, regular_grids
from src.d00_utils.gbounds import bbox_to_gdf
from src.d00_utils.system import get_system_memory, file_size
from src.d01_processing.export_raster import test_export_array as test_export
from src.d03_show.printers import print_attributes
from src.d00_utils.specs import create_raster_specs_from_path


# Function to suppress print statements
@contextlib.contextmanager
def suppress_stdout():
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        try:
            sys.stdout = devnull
            yield
        finally:
            sys.stdout = old_stdout


class MaskIt:

    def __init__(self, masking_raster: str, epsg_code: int,
                 raster_value: float, rawraster: str,
                 gridtype: str, terrain: str, batch_script: bool):
        self.masking_raster = masking_raster
        self.terrain = terrain
        self.epsg_code = epsg_code
        self.crs = f"EPSG:{epsg_code}"
        self.raster_value = raster_value
        self.raw_raster = rawraster
        self.grid_type = gridtype
        self.system_memory = get_system_memory()
        self.batch = batch_script
        self.max_size = 0
        assert isinstance(self.batch, bool)
        self.intmds_folder = self._init_intmds_folder()

        dask.config.set(**{'array.slicing.split_large_chunks': True})
        dask.config.set({"array.chunk-size": "256 MB"})
        calc_chunks = True

        self.output_paths = {"wse": ""}

        self.tile_gdf = None
        self.mask_specs = create_raster_specs_from_path(self.masking_raster)
        print(f'Mask Specs: ')
        print_attributes(self.mask_specs, 2)
        self.target_bounds = self._init_target_bounds()

        self.chunk_return = {"x": 2048, "y": 2048}
        if calc_chunks:
            chunk_return = self._init_chunk_calc()
            self.chunk_return = chunk_return

        self.terrain_variation = None
        self._init_max_size()

        assert self.grid_type in ["wse only", "wse depth", "depth and fill"]
        if "depth" not in self.grid_type:
            self.terrain = None
            print(f'  Ignoring terrain because producing {self.grid_type}')
        else:
            self.output_paths["depth"] = []
        self._init_terrain()

    def _init_max_size(self):
        raster_dict = {"mask": self.masking_raster, "analysis": self.raw_raster, "terrain": None}
        if self.terrain:
            raster_dict["terrain"] = self.terrain

        sizes = []
        for t, path in raster_dict.items():
            size_str, size_float, units = file_size(path)
            print(f" {t.upper()} file size: {size_str}")
            if t != "terrain":
                sizes.append(size_float)
        self.max_size = max(sizes)

    def _init_target_bounds(self):

        # Open and format arrays
        # Open, set extent for mask array
        print(f'Starting mask')
        if not self.batch:
            print(f'Finding overlapping extent')
            target_bounds = self.find_intersection_two_rasters(self.masking_raster, self.raw_raster,
                                                               test_folder=self.intmds_folder)
        else:
            print(f'Batch script: {self.batch}')
            with rasterio.open(self.masking_raster) as target_raster:
                target_bounds = target_raster.bounds
        return target_bounds

    def _init_chunk_calc(self):
        width, height = regular_grids.get_width_height(self.target_bounds)
        # print(f' Found w, h: {width}, {height}')
        cells_high = int(round(height / self.mask_specs.cellsizes[1]))
        cells_wide = int(round(width / self.mask_specs.cellsizes[0]))
        cells_wide += 1 if not cells_wide // 2 else cells_wide
        cells_high += 1 if not cells_high // 2 else cells_high

        print(f'  Cells wide: {cells_wide}, high: {cells_high}')

        parts_x = maths.find_largest_divisible_part(cells_wide, 4)
        parts_y = maths.find_largest_divisible_part(cells_high, 4)

        # Fix parts
        if parts_x == 0 or parts_x < 512:
            parts_x = 1028
        if parts_y == 0 or parts_y < 512:
            parts_y = 1028

        print(f'   Parts x: {parts_x}, y: {parts_y}')
        tile_gdf, axis_cells, number_cells = regular_grids.create_regular_grid(bounds=self.target_bounds,
                                                                               epsg_code=self.epsg_code,
                                                                               n_cells=(parts_x, parts_y))

        tile_gdf.to_file(os.path.join(self.intmds_folder, f"mask_chunks_{parts_x}_{parts_y}.shp"))
        self.tile_gdf = tile_gdf
        chunks = {"x": parts_x, "y": parts_y}
        print(f'Chunks: {chunks}')
        return chunks

    def _init_terrain(self):
        if self.terrain is not None:
            # Get variation
            print(f' TIFF Terrain Path: {self.terrain}')
            with rasterio.open(self.terrain) as src:
                stats = src.statistics(1, True)
                self.terrain_variation = stats.max - stats.min
        else:
            self.terrain = None

    def _init_intmds_folder(self):
        base, rawname = os.path.split(self.raw_raster)
        rawname = rawname.split(".")[0]
        intmd_folder = os.path.join(base, f"intmd_{rawname}")
        if not os.path.exists(intmd_folder):
            os.makedirs(intmd_folder)
        return intmd_folder

    @property
    def dataset_pbar(self):
        return tqdm(total=1,
                    desc="Processing Datasets",
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}")

    @property
    def tgt_rasterio_crs(self):
        if type(self.epsg_code) == int:
            # noinspection PyArgumentList
            crs_obj = CRS.from_epsg(self.epsg_code)

            return crs_obj

    @property
    def keeps_dict(self) -> dict:
        return {"vars": ["ground_elev", "ones_mask", "wse"],
                "dims": ["x", "y"],
                "indexes": ["x", "y"],
                "coords": ["x", "y", "spatial_ref"],
                "attrs": ["_Fill_Value"]}

    @staticmethod
    def rename_var_and_attr(ds, old: str, new: str):

        ds = ds.rename_vars({old: new})
        assign_dict = {"BandName": new, "long_name": new}
        att_list = [k for k, v in ds.attrs.items()]
        for k, v in assign_dict.items():
            if k in att_list:
                ds = ds.assign_attrs({k: assign_dict[k]})
        return ds

    @staticmethod
    def drop_ds_stuff(ds, keep_dict):

        ind_drops = [n for n, i in ds.indexes.items() if n not in keep_dict["indexes"]]
        var_drops = [n for n, v in ds.data_vars.items() if n not in keep_dict["vars"]]
        dim_drops = [n for n in ds.dims if n not in keep_dict["dims"]]

        ds = ds.drop_indexes(ind_drops)
        ds = ds.drop_vars(var_drops)
        ds = ds.drop_dims(dim_drops)
        return ds

    @staticmethod
    def find_intersection_two_rasters(rasterpath1, rasterpath2, test_folder: [str, None] = None):
        print(f'Finding raster intersection...')
        out_test = os.path.join(test_folder, "test_bounds.shp")
        print(f'Test location: {out_test}')
        with rioxr.open_rasterio(rasterpath1, band_as_variable=True, cache=False,
                                 masked=False) as ds1:
            crs = ds1.rio.crs
            with suppress_stdout():
                bbox1 = bbox_to_gdf(ds1.rio.bounds(), crs=crs)
            with rioxr.open_rasterio(rasterpath2, band_as_variable=True, cache=False,
                                     masked=False) as ds2:
                bounds2 = ds2.rio.bounds()
                with suppress_stdout():
                    bbox2 = bbox_to_gdf(bounds2, crs=crs)
                pg_intersection = bbox1.intersection(bbox2)
                intsct_bounds = pg_intersection.total_bounds
                print(f'Intersection Bounds:')
                for b in intsct_bounds:
                    print(f' {b}')
                bounds_gdf = bbox_to_gdf(intsct_bounds, crs)
                bounds_gdf.to_file(out_test)
                return intsct_bounds

    @staticmethod
    def window_generator(ds, window_size_x, window_size_y):
        """
        Find a sub-region (window) from the dataset that does not contain only nodata values.

        Parameters:
        - ds (xarray.Dataset): The input dataset.
        - window_size_x (int): The width of the window in number of cells.
        - window_size_y (int): The height of the window in number of cells.
        - nodata_value (float): The nodata value to check against.

        Returns:
        - xarray.Dataset: The selected sub-region of the dataset.
        """
        x_coords = ds.pg_coords['x'].values
        y_coords = ds.pg_coords['y'].values

        # Iterate over possible windows
        for i in range(0, len(x_coords) - window_size_x + 1, window_size_x):
            for j in range(0, len(y_coords) - window_size_y + 1, window_size_y):
                window = ds.isel(x=slice(i, i + window_size_x), y=slice(j, j + window_size_y))
                yield window

    def update_pbar(self, description=None):
        """
        Updates the tqdm progress bar (pbar) with a new description.
        If description is None, it simply increments the progress bar by 1.
        """
        if description:
            self.dataset_pbar.set_description(description)
        self.dataset_pbar.update(1)

    def open_and_align(self):
        from src.d01_processing.dask_reproject import DaskReproject

        mask_ds = rioxr.open_rasterio(self.masking_raster, band_as_variable=True, cache=False,
                                      masked=False, default_name="mask_values", chunks=self.chunk_return)
        for name, obj in mask_ds.data_vars.items():
            if "mask" not in name:
                mask_ds = mask_ds.rename_vars({name: "mask_values"})
        nodata = mask_ds["mask_values"].rio.nodata
        mask_ds = mask_ds.rio.pad_box(*self.target_bounds, constant_values=nodata)
        mask_ds = mask_ds.rio.clip_box(*self.target_bounds, crs=self.crs)

        # Handle mask metadata, etc
        if not mask_ds.rio.crs:
            mask_ds.rio.write_crs(self.crs, inplace=True)
        mask_attrs = mask_ds.attrs
        if len(self.keeps_dict["attrs"]) < 1:
            self.keeps_dict["attrs"] = mask_attrs
        mask_crs = mask_ds.rio.crs

        # Simplify mask ds
        print(f'Getting INT mask...')
        mask_array = mask_with_ones(mask_ds, "mask").chunk(self.chunk_return)
        mask_ds["ones_mask"] = mask_array
        mask_ds = mask_ds.drop_vars([v for v in mask_ds.rio.vars if v != "ones_mask"])
        # test_export(mask_ds["ones_mask"], self.intmds_folder, line=428)

        # Open analysis grid
        print(f'Getting input grid from {os.path.split(self.raw_raster)[1]} ...')
        grid_ds = rioxr.open_rasterio(self.raw_raster, band_as_variable=True, chunks=self.chunk_return)
        grid_ds = self.rename_var_and_attr(grid_ds, "band_1", "wse")
        grid_ds["wse"].rio.write_crs(self.crs, inplace=True)
        grid_ds["wse"].rio.write_nodata(-9999, inplace=True, encoded=False)
        grid_ds = grid_ds.rio.clip_box(*self.target_bounds, crs=mask_crs, auto_expand=True, auto_expand_limit=3)
        grid_ds = grid_ds.rio.pad_box(*self.target_bounds, constant_values=-9999).chunk(self.chunk_return)
        # test_export(grid_ds["wse"], self.intmds_folder, line=353)

        not_mask_ds_list = [grid_ds]

        # Open terrain (where applicable)
        if self.terrain is not None:
            print(f'Getting terrain grid from {os.path.split(self.terrain)[1]} ...')
            terrain_ds_xr = rioxr.open_rasterio(self.terrain, band_as_variable=True, chunks=self.chunk_return)
            terrain_ds_xr = terrain_ds_xr.rename_vars({"band_1": "ground_elev"})
            terrain_ds_xr.rio.write_crs(self.crs, inplace=True)
            terrain_ds_xr = self.drop_ds_stuff(terrain_ds_xr, self.keeps_dict)
            terrain_ds_xr = terrain_ds_xr.rio.pad_box(*self.target_bounds, constant_values=-9999)
            terrain_ds_xr = terrain_ds_xr.rio.clip_box(*self.target_bounds, crs=self.crs).chunk(self.chunk_return)
            not_mask_ds_list.append(terrain_ds_xr)
            # min_value = terrain_ds_xr.ground_elev.where(terrain_ds_xr.ground_elev != -9999).min().values
            # max_value = terrain_ds_xr.ground_elev.max().values
            # print(f'Terrain: \n----\n{min_value} to {max_value}')
            sleep(2)
            # test_export(terrain_ds_xr.ground_elev, self.intmds_folder, line=368)

        # Re-align the datasets
        print(f'Aligning datasets to {os.path.split(self.masking_raster)[1]} ...')
        with LocalCluster(memory_limit='100GB') as cluster:  # Adjust memory limit as needed
            client = Client(cluster)
            with self.dataset_pbar as dataset_pbar:
                reprojected = []
                for ds in not_mask_ds_list:
                    dataset_name = [k for k in ds.data_vars.keys()][0]
                    current_description = dataset_pbar.desc
                    desctoadd = f"\nProcessing {dataset_name}"
                    dataset_pbar.set_description(current_description + desctoadd)
                    reprojected_vars = {var: DaskReproject(ds[var], mask_ds.ones_mask,
                                                           window_gdf=self.tile_gdf,
                                                           chunk_win=self.chunk_return).apply_reproject_match() for var
                                        in ds.data_vars}
                    reprojected_ds = xr.Dataset(reprojected_vars, coords=mask_ds.pg_coords)
                    reprojected.append(reprojected_ds)
                    dataset_pbar.update(1)
                client.close()

        # Combine the reprojected datasets by coordinates
        comb_ds = xr.combine_by_coords(reprojected)

        # Cleanup attributes and variables n stuff
        comb_ds = self.drop_ds_stuff(comb_ds, self.keeps_dict).chunk(self.chunk_return)

        # Handle no data
        print(f'455 DS: \n{comb_ds}')
        print("No-data values for each ds variable:")
        for dvar in comb_ds.rio.vars:
            if {dvar}.isdisjoint({"ground_elev", "ones_mask"}):
                comb_ds[dvar].rio.write_nodata(-9999, inplace=True, encoded=False)
            else:
                comb_ds[dvar].rio.write_nodata(-1, inplace=True, encoded=False)
            mn, mx = comb_ds[dvar].min().values, comb_ds[dvar].max().values
            print(f"488 {dvar} Variable, Min: {mn}, Max: {mx}")
            print(f"  - {dvar} new: {comb_ds[dvar].rio.nodata}")
            test_export(grid_ds["wse"], self.intmds_folder, line=418)

        print(f'\n Combined ({len(comb_ds.data_vars)}): \n----\n{comb_ds}')  # {comb_ds.chunks}

        return comb_ds

    # noinspection PyUnresolvedReferences
    @staticmethod
    def apply_nodata_mask(mask_array: xr.DataArray, targ_array: xr.DataArray,
                          mask_nodatavalue: [int, float], targ_nodatavalue: [int, float]):

        # Ensure both rasters have the same dimensions and coordinates
        assert mask_array.shape == targ_array.shape, "Rasters must have the same shape"
        assert all(mask_array.coords[dim].equals(targ_array.coords[dim]) for dim in
                   mask_array.dims), "Rasters must have the same coordinates"

        # Identify nodata values in the first raster
        # noinspection PyTypeChecker
        nodata_mask: xr.DataArray = mask_array == mask_nodatavalue
        true_count = nodata_mask.where(nodata_mask == True).count().values
        false_count = nodata_mask.where(nodata_mask == False).count().values
        print(f"Count of True: {true_count}")
        print(f"Count of False: {false_count}")

        # Apply the nodata mask to the second raster
        return targ_array.where(~nodata_mask, targ_nodatavalue)

    def depth_fill_n_mask(self, ds):
        print(f"Filling and masking input grid (creating filled depth grid)...")

        # Create depth grid array
        comb_ds = ds
        comb_ds.rio.set_crs(self.crs, inplace=True)
        comb_ds.rio.write_crs(self.crs, inplace=True)
        # comb_ds['fill_box'] = xr.full_like(comb_ds['ones_mask'], fill_value=self.raster_value, dtype='int32')
        comb_ds['depth'] = (comb_ds['wse'] - comb_ds['ground_elev']).where(comb_ds['ones_mask'] == 1)
        comb_ds['depth'] = comb_ds['depth'].where(
            comb_ds['depth'] > 0)
        print(f'Raw Depth: \n----\n{comb_ds["depth"]}')
        comb_ds['depth_box'] = comb_ds['depth'].fillna(self.raster_value)
        comb_ds.drop_vars('depth')
        # comb_ds["depth_box"] = self.fill_nodata_with_value(comb_ds['depth'], fill_value=self.raster_value)
        comb_ds['depth_box'].rio.set_nodata(-9999, inplace=True)
        comb_ds['depth_box'].rio.set_crs(self.crs, inplace=True)
        comb_ds.rio.write_crs(self.crs, inplace=True)
        comb_ds['depth_box'].rio.write_nodata(-9999, inplace=True)

        # Mask-out
        print(f' Filling input raw grid...({comb_ds["depth"].rio.nodata})')
        # test_export(comb_ds['ones_mask'], self.intmds_folder)
        comb_ds['depth'] = xr.where(comb_ds['ones_mask'] == 1, comb_ds["depth_box"],
                                    -9999)  # Put the box (only) where the mask is
        comb_ds['depth'] = xr.where(comb_ds["depth"] < (abs(self.terrain_variation) / 2), comb_ds["depth"],
                                    -9999)  # Remove extremes -- based on terrain elevation variation
        comb_ds['depth'].rio.set_nodata(-9999, inplace=True)
        comb_ds['depth'].rio.write_nodata(-9999, inplace=True)
        comb_ds['depth'].rio.set_crs(self.crs, inplace=True)
        comb_ds.rio.write_crs(self.crs, inplace=True)
        depth_ds = comb_ds.drop_vars([v for v in comb_ds.rio.vars if v != "depth"])
        print(f'Processed Depth: \n----\n{comb_ds["depth"]}')
        print(f' Masked input with mask')

        # Output naming
        base = self.intmds_folder
        outpath = os.path.join(base, f"depth_FILLED.tif")
        if not os.path.exists(outpath):
            depth_ds['depth'].rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                            windowed=True, bigtiff="YES")
        self.output_paths["depth"].append(outpath)

    def wse_depth_masking(self, in_ds):
        print(f'\n Not Filling...')  # Create the mask
        comb_ds = in_ds
        onescount = comb_ds.ones_mask.where(comb_ds.ones_mask == 1).count().values
        notonecount = comb_ds.ones_mask.where(comb_ds.ones_mask != 1).count().values
        print(f' Ones: {onescount}, ~Ones: {notonecount}')
        inextent = comb_ds.rio.bounds()
        gdf = bbox_to_gdf(inextent, crs=self.crs, outfolder=self.intmds_folder, name_str="inbbox")
        ones_mask = comb_ds.ones_mask == 1

        # where for each variable
        print(f'\nMasking by mask...')
        for var in comb_ds.rio.vars:
            if "mask" not in var:
                test_export(comb_ds[var], self.intmds_folder, line=575)
                comb_ds[var] = comb_ds[var].where(comb_ds.ones_mask == 1, other=-9999)
                comb_ds[var].rio.write_nodata(-9999, inplace=True)
                test_export(comb_ds[var], self.intmds_folder, line=578)

        if "depth" in self.grid_type:
            print("Creating depth grid also")
            # Create depth grid array
            comb_ds.rio.write_crs(self.crs, inplace=True)
            # Create depth
            comb_ds['depth'] = comb_ds['wse'] - comb_ds['ground_elev']
            # Filter depth
            comb_ds['depth'] = comb_ds['depth'].where(comb_ds['depth'] > 0, other=-9999)

            # Handle meta
            comb_ds['depth'].rio.write_nodata(-9999, inplace=True)
            comb_ds['depth'].rio.set_crs(self.crs, inplace=True)
            comb_ds.rio.write_crs(self.crs, inplace=True)
            print(f'Depth Mean: \n----\n{comb_ds["depth"].where(comb_ds.depth > 0).mean().values}')

            # Mask else by depth > 0
            comb_ds = xr.where(comb_ds["depth"] > 0, comb_ds, -9999)

        print(f'\nMasked DS: \n{comb_ds}')
        self.export_rasters(ds=comb_ds)

    def export_rasters(self, ds):
        for varname in ds.rio.vars:

            exp_array = ds[varname].fillna(-9999)
            outpath = os.path.join(self.intmds_folder, f"{varname}_masked.tif")
            # Ensure the array only has 2 dimensions
            assert len(exp_array.dims) == 2, f"Unexpected number of dimensions: {exp_array.dims}"

            # Check the shape and chunks of the array
            print(f"Shape: {exp_array.shape}")
            # print(f"Chunks: {exp_array.chunks}")

            # Ensure the array is properly chunked
            exp_array = exp_array.chunk({"x": 2048, "y": 2048})

            # Write nodata and CRS to the array
            exp_array = exp_array.rio.write_nodata(-9999, inplace=True)
            exp_array = exp_array.rio.write_crs(self.crs, inplace=True)

            print(f"Array after processing: ---\n{exp_array}")
            print(f"  Output has: --- {len(exp_array.shape)} --- dims")
            print(f"  Output has: --- {len(exp_array.dims)} --- indices")
            # print(f"  Coordinates: {exp_array.coords}")

            # Proceed if array is not empty or does not contain only nodata values
            try:
                print(f'++++Exporting to: \n   {outpath}')
                # Use dask to write the array in chunks
                exp_array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                        windowed=True, bigtiff="YES")

                self.output_paths[varname] = outpath
            except IndexError as ie:
                print(f"IndexError: {ie}")
                if os.path.exists(outpath):
                    os.remove(outpath)

    @staticmethod
    def define_proj(path, epsg_code):

        if os.path.exists(path):
            try:
                with rasterio.open(path, "r+") as raster:
                    if epsg_code is not None:
                        # noinspection PyArgumentList
                        raster.crs = CRS.from_epsg(epsg_code)
                        print(f' {path} Defined with: {epsg_code}')
            except TypeError as te:
                print(f' No file, {path}\n{te}')

    def run_scenarios(self) -> tuple[dict, rasterio.crs.CRS()]:
        from src.d00_utils.check_crs_match import check_crs_match_from_list
        if "only" in self.grid_type:
            self.terrain = None
        rasterstocheck = [r for r in [self.raw_raster, self.masking_raster, self.terrain] if r is not None]
        for path in rasterstocheck:
            print(f"  {path}")
        crs_check = check_crs_match_from_list(rasterstocheck)
        if not crs_check:
            return dict(), self.crs
        else:
            ds = self.open_and_align()
            if self.grid_type in ['wse depth', "wse only"]:
                self.wse_depth_masking(ds)
            elif self.grid_type == 'depth and fill':
                self.depth_fill_n_mask(ds)
            for var_type, path in self.output_paths.items():
                print(f' Defining CRS for {var_type}, {path}')
                self.define_proj(path, epsg_code=self.epsg_code)

        return self.output_paths, self.crs


if __name__ == "__main__":
    # Example usage
    st = time()
    masking_raster_path = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\test_WSE_0_2pct_DD.tif"
    terrain_path = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\Terrain\terrain_Lower_Cedar_vFT_3418.tif"
    epsg = 3418
    fillvalue = 0.0001
    raster_file = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\source_01pct.tif"
    grid_type: str = "depth and fill"
    ## ["depth and fill", "wse depth", "wse only"]

    init = MaskIt(masking_raster_path, epsg, fillvalue, raster_file, grid_type, terrain=terrain_path,
                  batch_script=False)
    print(f'Start time: {st}')
    outputs = init.run_scenarios()
    end = time()
    elapsed = round(round(end - st, 1) / 60, 1)
    print(f"Outputs")
    for k, v in outputs[0].items():
        print(f'{k}: {v}')
    print(f'\nRun Time: {elapsed} minutes')
