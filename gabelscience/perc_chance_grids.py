import threading
import numpy as np
from math import log10
import rioxarray as rioxr
import os
import typing as T
from sparse import COO
import xarray as xr
from src.specs.raster_specs import create_raster_specs_from_path, create_raster_specs_from_xarray
import logging
from src.d01_processing.export_raster import export_raster, test_export_array
from time import process_time
import dask.config
import dask.array as da
from dask.diagnostics import ProgressBar
from src.d00_utils.bounds_convert import bbox_to_gdf
import matplotlib.pyplot as plt



def setup_logger():
    # Create a custom logger
    module_name = __name__ if __name__ != "__main__" else os.path.splitext(os.path.basename(__file__))[0]
    logger = logging.getLogger(module_name)
    # Set the level of this logger. DEBUG is the lowest severity level.
    logger.setLevel(logging.DEBUG)
    # Create handlers
    file_handler = logging.FileHandler(os.path.join(os.getcwd(), f'{module_name}.log'))
    # Create formatters and add it to handlers
    log_fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_fmt)
    # Add handlers to the logger
    logger.addHandler(file_handler)
    return logger


# then call this function:
logger = setup_logger()

# Define the ranges and corresponding float values
ranges = {
    (0, 0.002): 0.002,
    (0.002, 0.01): 0.010,
    (0.01, 0.02): 0.020,
    (0.02, 0.04): 0.040,
    (0.04, 0.1): 0.100,
    # Add more ranges as needed
}


def categorize_value(x):
    for (low, high), value in ranges.items():
        if low < x <= high:
            return value
        else:
            return np.nan  # or some default value


def convert_to_categorical(data_array):
    def vectorized_categorize(x):
        return np.vectorize(categorize_value)(x)

    categorized_array = xr.apply_ufunc(
        vectorized_categorize,
        data_array,
        vectorize=True,
        dask='parallelized',
        output_dtypes=[np.float32]
    )

    with ProgressBar():
        categorized_array = categorized_array.compute()

    return categorized_array


def compute_unique_values(data_array, n, p, timeout=15):
    def unique_block(block):
        return np.unique(block)

    dask_array = data_array.data
    unique_values = set()
    blocks_processed = 0
    new_unique_found = True
    start_time = time.time()

    with ProgressBar():
        while new_unique_found and blocks_processed < dask_array.npartitions:
            new_unique_found = False
            for i in range(p):
                if blocks_processed >= dask_array.npartitions:
                    break
                if time.time() - start_time > timeout:
                    print(f"Timeout after {timeout} seconds")
                    return list(unique_values)
                block = dask_array.blocks[blocks_processed].compute()
                unique_block_values = unique_block(block)
                new_values = set(unique_block_values) - unique_values
                if new_values:
                    unique_values.update(new_values)
                    new_unique_found = True
                blocks_processed += 1
                if len(unique_values) >= n:
                    break

    return list(unique_values)


def find_rasters(path):
    """
    Find the rasters in the given path
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".tif"):
                yield os.path.join(root, file)


def print_xarray_stats(array):
    """
    Print the xarray stats
    """
    print(f"\tSTD: {array.std().values}")
    print(f"\tMEAN: {array.mean().values}")
    print(f"\tMAX: {array.max().values}")
    print(f"\tMIN: {array.min().values}")
    # print(f"COUNT: {np.round(array.count().values, 3)}")


def drop_atts(array, atts):
    """
    Drop the attributes
    """
    current_atts = {k: v for k, v in array.attrs.items()}
    for att in current_atts:
        if att in atts:
            array.attrs.pop(att, None)
    return array


def update_stats_atts(array):
    """
    Update the stats attributes
    """
    stats_mask = array.where(array != array.rio.nodata)
    stats = {"STATISTICS_STDDEV": np.round(stats_mask.std().values, 2),
             "STATISTICS_MEAN": np.round(stats_mask.mean().values, 2),
             "STATISTICS_MAXIMUM": np.round(stats_mask.max().values, 2),
             "STATISTICS_COUNT": np.round(stats_mask.count().values, 2),
             "STATISTICS_MINIMUM": np.round(stats_mask.min().values, 2), }
    array.assign_attrs(stats)
    return array


def what_is_next_higher(current_return):
    """
    Get the next higher return
    """
    return_number = [k for k, v in RETURNS.items() if v == current_return][0]

    # Get higher
    if not return_number == 10:
        higher_number = return_number + 1
    else:
        higher_number = 10

    return RETURNS.get(higher_number, return_number)


def what_is_next_lower(current_return):
    """
    Get the next lower return
    """
    return_number = [k for k, v in RETURNS.items() if v == current_return][0]

    # Get lower
    if not return_number == 6:
        lower_number = return_number - 1
    else:
        lower_number = 6

    return RETURNS.get(lower_number, return_number)


def get_higher_lower_pct_return(pct_grids: xr.Dataset, varname: str, *args, **kwargs):
    min_return, max_return = min(RETURNS.keys()), max(RETURNS.keys())

    # Get lower return percentages
    for i, (return_ord, return_pct) in enumerate(RETURNS.items()):
        if return_ord == min_return:
            lower_return = return_pct
        else:
            lower_return = RETURNS.get(return_ord - 1)

        # Get equiv locs
        equiv = pct_grids[varname] == return_pct

        if i == 0:
            pct_grids['NextLowestPct'] = xr.where(equiv, lower_return, pct_grids[varname])
        else:
            pct_grids['NextLowestPct'] = xr.where(equiv, lower_return, pct_grids['NextLowestPct'])

    # Get higher array
    for i, (return_ord, return_pct) in enumerate(RETURNS.items()):
        if return_ord == max_return:
            higher_return = return_pct
        else:
            higher_return = RETURNS.get(return_ord + 1)

        # Get equiv locs
        equiv = pct_grids[varname] == return_pct

        if i == 0:
            pct_grids['NextHighestPct'] = xr.where(equiv, higher_return, pct_grids[varname])
        else:
            pct_grids['NextHighestPct'] = xr.where(equiv, higher_return, pct_grids['NextHighestPct'])

    return pct_grids


RETURNS = {10: 0.002, 9: 0.010, 8: 0.020,
           7: 0.040, 6: 0.100}

RETURN_LOOKUP = {v: k for k, v in RETURNS.items()}

RETURN_NAMES = {"_0_2pct": 10, "_01pct": 9, "_02pct": 8,
                "_04pct": 7, "_10pct": 6}

RETURN_NAME_LOOKUP = {v: k for k, v in RETURN_NAMES.items()}

OUTPUT_SUBFOLDERS = ["00_DEM_Masked", "01_Elev_Grids", "02_WSE_Nearest_Ground", "03_PAC", "__INTMD__"]


class PercChanceGrids:
    def __init__(self, input_path, output_path, dem):
        self.input_path = input_path
        self.output_path = output_path
        self.input_dem_path = dem

        self.return_paths = {}
        self.xarrays_ready = {}
        self.valid_gdf = None
        self.dem = xr.DataArray
        self.analysis_ds = xr.Dataset
        self.high_low_ds = xr.Dataset
        self.valid_loc = None

        # Find the WSE rasters
        self._find_wse_rasters()
        self._value_raster_specs = create_raster_specs_from_path(self.return_paths[10])
        self._all_raster_specs = {}
        self._set_configs()
        logger.info(f"\n\nNEW RUN\n\n")
        logger.info(f"Value Raster Specs: {self.vspecs}")

    @property
    def vspecs(self):
        return self._value_raster_specs

    @property
    def all_specs(self):
        return self._all_raster_specs

    def add_specs(self, key, path=None, array=None, raster_object=None):
        if path:
            self._all_raster_specs[key] = create_raster_specs_from_path(path)
        elif isinstance(array, xr.DataArray):
            self._all_raster_specs[key] = create_raster_specs_from_xarray(array)
        else:
            self._all_raster_specs[key] = raster_object

    def _init_all_raster_specs(self):
        for key, path in self.return_paths.items():
            self._all_raster_specs[f"IN_{key}"] = create_raster_specs_from_path(path)

    @property
    def output_subfolders(self):

        return self._subfolder_paths()

    def _subfolder_paths(self):
        return [os.path.join(self.output_path, f) for f in OUTPUT_SUBFOLDERS]

    @property
    def dem_path(self):
        masked_dem_path = os.path.join(self.output_subfolders[0], "dem_masked.tif")
        # if os.path.exists(masked_dem_path):
        #     return masked_dem_path
        # else:
        return self.input_dem_path

    def _find_wse_rasters(self):
        for path in find_rasters(self.input_path):
            name = os.path.basename(path)
            if "WSE" not in name:
                continue
            else:
                for key, value in RETURN_NAMES.items():
                    if key in name:
                        self.return_paths[value] = path
                        break

    def _set_configs(self):
        dask.config.set(**{'array.slicing.split_large_chunks': False})

    def open_valid_xarray(self):
        """
        Open the valid raster and DEM
        """
        atts = {"crs": self.vspecs.crs, "BandName": "WSE",
                "long_name": "Water Surface Elevation 0.2%", "DataType": "Elevation"}
        valid_xarray = (rioxr.open_rasterio(self.return_paths[10],
                                            chunks={"x": 2048, "y": 2048})
                        .rename("WSE_0_2pct").assign_attrs(atts))

        # Round it to 2 decimal places
        valid_xarray = valid_xarray.round(2)

        # Update the stats attributes
        valid_xarray = update_stats_atts(valid_xarray)

        # Export the valid raster
        valid_xarray.rio.write_nodata(-9999, inplace=True, encoded=False)
        export_raster(valid_xarray, os.path.join(self.output_subfolders[1], "WSE_0_2pct.tif"))

        # Store TF valid locations
        self.valid_loc = xr.where(valid_xarray != -9999, True, False)
        test_valid = xr.where(self.valid_loc, 1, 99)
        test_export_array(test_valid, self.output_subfolders[4], 329)
        valid_xarray = valid_xarray.where(self.valid_loc)

        vbounds = valid_xarray.rio.bounds()
        self.valid_gdf = bbox_to_gdf(vbounds, self.vspecs.crs, "valid_bounds", self.output_path)

        return valid_xarray

    def open_dem_xarray(self, varray):
        # Open DEM
        print(f"DEM path: {self.dem_path}")
        dem = rioxr.open_rasterio(self.dem_path, chunks={"x": 2048, "y": 2048}).rename("DEM")

        # Assign attributes
        atts = {"crs": dem.rio.crs, "BandName": "DEM",
                "long_name": "Digital Elevation Model", "DataType": "Elevation", "_FillValue": -9999}
        dem = dem.assign_attrs(atts)
        for k, v in {k: v for k, v in dem.attrs.items()}.items():
            if k not in atts:
                dem.attrs.pop(k, None)

        # Clip the DEM to the valid raster
        dem = dem.rio.clip(self.valid_gdf.geometry)
        # dem = dem.rio.pad_box(*self.vspecs.bounds, constant_values=dem.rio.nodata)

        # align the DEM and valid raster
        print(f'Aligning DEM and valid raster')
        dem.reindex_like(varray, method="nearest", tolerance=1.5).chunk({"x": 2048, "y": 2048})
        print(f"Reindexed DEM shape: {dem.shape}")
        print(f"Other SHape: {varray.shape}")

        # Mask the DEM by valid raster
        dem = dem.where(self.valid_loc)
        print(f"Masked DEM SHape: \n{dem.shape}")
        print(f"Other Shape: \n{varray.shape}")

        # Round the DEM to 2 decimal places
        dem = dem.round(2)
        dem = update_stats_atts(dem)

        # Export the masked DEM
        dem.rio.write_nodata(-9999, inplace=True, encoded=False)
        oupath = os.path.join(self.output_subfolders[0], "DEM_masked.tif")
        if not os.path.exists(oupath):
            export_raster(dem, oupath)
        dem = dem.where(dem != -9999)

        # Export bounds for storage
        bounds = dem.rio.bounds()
        bbox_to_gdf(bounds, self.vspecs.crs, "dem_bounds", self.output_subfolders[4])

        # Convert to dataset
        self.analysis_ds = dem.to_dataset(name="Ground")

        # Add 0.2% WSE to the dataset
        self.analysis_ds["WSE_0_2pct"] = varray
        print(f"\n391 Analysis DS: \n{self.analysis_ds}")
        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})
        self.analysis_ds = self.analysis_ds.drop_vars([k for k in self.analysis_ds.data_vars.keys() if k not in
                                                       ["Ground", "WSE_0_2pct"]])

        # Handle coordinates, dims, and vars
        if "band" in self.analysis_ds.dims:
            self.analysis_ds = self.analysis_ds.drop_vars("band")
            self.analysis_ds = self.analysis_ds.squeeze("band", drop=True)
            for varname in self.analysis_ds.data_vars.keys():
                if len(self.analysis_ds[varname].shape) == 2:
                    continue
                else:
                    self.analysis_ds[varname] = self.analysis_ds[varname].isel(band=0, drop=True)
        for var, _ in self.analysis_ds.data_vars.items():
            print(f"\tVariable: {var}, {round(self.analysis_ds[var].nbytes / 1e9, 2)} GB")
            if var == "Ground":
                self.add_specs(f"OPEN_{var}", array=self.analysis_ds[var])
            self.add_specs(f"OPEN_{var}", array=self.analysis_ds[var])

        # Delete the antecedent arrays to free up resources
        del dem, varray

        print(f"\n405 Analysis DS: \n{self.analysis_ds}")
        for varname in self.analysis_ds.data_vars.keys():
            self.analysis_ds[varname].rio.write_nodata(-9999, inplace=True, encoded=False)
            export_raster(self.analysis_ds[varname], os.path.join(self.output_subfolders[1], f"{varname}.tif"))
            self.analysis_ds[varname] = self.analysis_ds[varname].where(self.valid_loc)
        print(f"\nAnalysis DS: \n{self.analysis_ds}")

    def open_wse_xarrays(self):
        """
        Open the WSE xarrays
        """
        for key, path in self.return_paths.items():
            if key == 10:
                continue

            dsw = rioxr.open_rasterio(path, band_as_variable=True, lock=False, chunks={"x": 2048, "y": 2048})

            # Rename the variable
            new_varname = f"WSE{RETURN_NAME_LOOKUP.get(key, 'WSE_UNK')}"
            dsw = dsw.rename_vars({b: new_varname for b in dsw.data_vars.keys() if "band" in b.lower()})
            # Remove the 'band' dimension
            # dsw = dsw.squeeze('band', drop=True)
            # Expand the dimensions of the data variable to add a new dimension with size 1
            # dsw[new_varname] = dsw[new_varname].expand_dims(dim='WSE', axis=0)
            dsw = dsw.drop_vars([k for k in dsw.data_vars.keys() if k != new_varname])

            # Records the specs
            print(f"\nRETURN OPENED: {dsw}")
            print(f"\tOpened {new_varname} {round(dsw.nbytes / 1e9, 2)} GB")
            print(f"\tShape: {dsw[new_varname].shape}")
            self.add_specs(f"OPEN_{key}", array=dsw)
            logger.info(f" SPECS {self.all_specs[f'OPEN_{key}']}")
            atts = {"crs": dsw.rio.crs, "BandName": RETURN_NAME_LOOKUP.get(key, None),
                    "long_name": f"Water Surface Elevation {RETURNS.get(key, None) * 100}%",
                    "DataType": "Elevation"}
            pre_attrs = {k: v for k, v in dsw.attrs.items()}
            for k, v in pre_attrs.items():
                if k in atts:
                    dsw.attrs[k] = atts[k]
                else:
                    dsw.attrs.pop(k, None)

            # Clip to relevant extent
            no_data = dsw[new_varname].attrs["_FillValue"] if "_FillValue" in dsw[new_varname].attrs else -9999
            if len(dsw.data_vars) > 1:
                raise ValueError("More than one variable in the dataset")
            elif len(dsw.data_vars) == 0:
                raise ValueError("No variables in the dataset")
            dsw = dsw.rio.clip(self.valid_gdf.geometry)
            dsw[new_varname].rio.write_nodata(no_data, inplace=True, encoded=False)
            test_export_array(dsw[new_varname], self.output_subfolders[4], 444)

            # Reindex to the DEM
            dsw = dsw.reindex_like(self.analysis_ds["Ground"], method="nearest", tolerance=1.5)
            if dsw[new_varname].shape != self.analysis_ds["Ground"].shape:
                print(dsw)
                print(dsw[new_varname].where(dsw[new_varname != -9999]).values)
                raise ValueError(
                    f"Shapes do not match: {dsw[new_varname].shape} and {self.analysis_ds['Ground'].shape}")
            dsw[new_varname] = dsw[new_varname].rio.write_nodata(no_data, inplace=True, encoded=False)
            dsw.rio.write_crs(self.vspecs.crs, inplace=True)

            # Round the WSE to 1 decimal places
            dsw = dsw.round(2)
            # test_export_array(dsw[new_varname], self.output_subfolders[4], 335)
            self.analysis_ds = xr.combine_by_coords([self.analysis_ds, dsw],
                                                    join="outer", combine_attrs="drop_conflicts")
        print(f"\nDS: \n{self.analysis_ds}")
        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})
        self.analysis_ds = drop_atts(self.analysis_ds, ["BandName", "long_name"])
        # Handle some nodata
        for varname, darr in self.analysis_ds.data_vars.items():
            print(f"\n Variable: {varname}, {self.analysis_ds[varname].nbytes / 1e9} GB")
        print(f"\nDS: \n{self.analysis_ds}")

    def fill_wse_with_dem(self):
        filler = self.analysis_ds["Ground"]
        fill_subtract = 0
        for varname, dv in self.analysis_ds.data_vars.items():
            fill_subtract += 0.01
            print(f"\n Variable: {varname}")

            if "WSE" in varname:
                darr = self.analysis_ds[varname]
                print(f"\nFilling {varname} where it is null")
                print(f"{darr.name} : {darr.shape}")
                where_test = xr.where((darr > 0), 1, 0)
                where_test.rio.write_nodata(-9999, inplace=True, encoded=False)
                print(f'\tnodata: {darr.rio.nodata}\n\tencoded: {darr.rio.encoded_nodata}')

                filled = xr.where(where_test == 0, filler - fill_subtract, darr)
                print(f" Filler elevation: {fill_subtract}")
                filled.rio.write_nodata(-9999, inplace=True, encoded=False)

                self.analysis_ds[varname] = filled

                # Export the filled WSE
                oupath = os.path.join(self.output_subfolders[1], f"{varname}_filled.tif")
                outarr = self.analysis_ds[varname]
                outarr.rio.to_raster(oupath, tiled=True, lock=threading.Lock(), compress='LZW',
                                     windowed=True, bigtiff="YES")

        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})

    def create_pct_start_grid(self):

        for varname in self.analysis_ds.data_vars.keys():
            if "WSE" not in varname:
                continue
            print(f"\nProcessing {varname} to get percent chance based on its return extent")

            # Get % chance from varname
            perc_chance_name = varname.split("WSE")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                print(f'\nNo percent chance found for {varname}')
                continue
            print(f"\tPercent Chance: {perc_chance}, {type(perc_chance)}")

            pct_chance_array = (xr.where(self.analysis_ds[varname] > 0, perc_chance, np.nan)
                                .rename(f"pct_chance_{varname}")).astype(np.float32)
            # test_export_array(pct_chance_array, self.output_subfolders[4], 424, -9999)

            # Add the pct chance array to the dataset
            self.analysis_ds[f"pct_{varname}"] = pct_chance_array

        self.high_low_ds = self.analysis_ds.drop_vars(
            [k for k in self.analysis_ds.data_vars.keys() if "pct_" not in k]).chunk({"x": 2048, "y": 2048})
        self.analysis_ds = self.analysis_ds.drop_vars([k for k in self.analysis_ds.data_vars.keys() if "pct_" in k])
        print(f'\nPercent Chance Grids: \n{self.high_low_ds}')
        print(f'\nAnalysis DS: \n{self.analysis_ds}')

        stacked_chance = self.high_low_ds.to_array(dim="pct_start")
        max_pct = stacked_chance.max(dim="pct_start")
        self.high_low_ds["pct_start"] = max_pct
        self.high_low_ds = self.high_low_ds.drop_vars(
            [k for k in self.high_low_ds.data_vars.keys() if k != "pct_start"])
        self.high_low_ds = self.high_low_ds.chunk({"x": 2048, "y": 2048})

        # Get some stats
        print(f"\n\nPercent Start--")
        print(self.high_low_ds)
        # print_xarray_stats(self.high_low_ds["pct_start"])

        # Export the pct start
        self.high_low_ds["pct_start"].rio.write_nodata(-9999, inplace=True, encoded=False)
        export_raster(self.high_low_ds["pct_start"], os.path.join(self.output_subfolders[2], "pct_start.tif"))
        self.high_low_ds["pct_start"] = self.high_low_ds["pct_start"].where(self.high_low_ds["pct_start"] != -9999)

    def compute_next_high_next_low(self):

        # Compute next highest and lowest perc-chance return periods for each location
        self.high_low_ds = xr.map_blocks(func=get_higher_lower_pct_return, obj=self.high_low_ds, args=('pct_start',))
        print(f"Ann. % Chance Variables: \n\t{self.high_low_ds.data_vars.keys()}")

        # Fill some stuff
        lowest_return_pct = RETURN_LOOKUP.get(min(RETURNS.keys()), None)
        fill_where = self.high_low_ds["NextLowestPct"].isnull() & self.valid_loc
        self.high_low_ds["NextLowestPct"] = xr.where(fill_where, lowest_return_pct, self.high_low_ds["NextLowestPct"])

        highest_return_pct = RETURN_LOOKUP.get(max(RETURNS.keys()), None)
        fill_where = self.high_low_ds["NextHighestPct"].isnull() & self.valid_loc
        self.high_low_ds["NextHighestPct"] = xr.where(fill_where, highest_return_pct,
                                                      self.high_low_ds["NextHighestPct"])

        # Round them
        # self.high_low_ds["NextHighestPct"] = convert_to_categorical(self.high_low_ds["NextHighestPct"])
        # self.high_low_ds["NextLowestPct"] = convert_to_categorical(self.high_low_ds["NextLowestPct"])

        # Export the next highest and lowest
        self.high_low_ds = self.high_low_ds.chunk({"x": 2048, "y": 2048})
        for var_name in self.high_low_ds.data_vars.keys():
            self.high_low_ds[var_name] = self.high_low_ds[var_name].rio.write_nodata(-9999, inplace=True, encoded=False)
            export_raster(self.high_low_ds[var_name], os.path.join(self.output_subfolders[2], f"{var_name}.tif"))
            self.high_low_ds[var_name] = self.high_low_ds[var_name].where(self.high_low_ds[var_name] != -9999)

    def compute_next_high_next_low_elev(self):

        # Init next highest and lowest elevation arrays
        self.high_low_ds["NextHighestElevation"] = xr.ones_like(self.valid_loc, dtype=np.float32)
        self.high_low_ds["NextHighestElevation"] = self.high_low_ds["NextHighestElevation"].where(self.valid_loc)
        self.high_low_ds["NextLowestElevation"] = xr.ones_like(self.valid_loc, dtype=np.float32)
        self.high_low_ds["NextLowestElevation"] = self.high_low_ds["NextLowestElevation"].where(self.valid_loc)

        next_low_test = self.high_low_ds["NextLowestElevation"].rio.write_nodata(-9999, inplace=True, encoded=False)
        test_export_array(next_low_test, self.output_subfolders[4], 562)

        for i, varname in enumerate(self.analysis_ds.data_vars.keys()):
            if "WSE" not in varname:
                continue
            print(f"\nProcessing {varname} to get next highest and lowest elevations")

            # Get % chance from varname
            perc_chance_name = varname.split("WSE")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                continue

            # Find matching locations
            tolerance = 1e-2
            high_diff = abs(self.high_low_ds["NextHighestPct"] - perc_chance)
            low_diff = abs(self.high_low_ds["NextLowestPct"] - perc_chance)
            pct_high_match = xr.where(high_diff < tolerance, 1, 0)
            pct_low_match = xr.where(low_diff < tolerance, 1, 0)
            num_high_cells = pct_high_match.where(pct_high_match == 1).count().values
            num_low_cells = pct_low_match.where(pct_low_match == 1).count().values
            print(f"\tAnn {perc_chance * 100}% Chance: {perc_chance} cells, H: {num_high_cells:}, L: {num_low_cells:}")

            # Get the elevations
            if i == 0:
                self.high_low_ds["NextHighestElevation"] = xr.where(pct_high_match,
                                                                    self.analysis_ds[varname],
                                                                    self.analysis_ds["WSE_0_2pct"])
                self.high_low_ds["NextLowestElevation"] = xr.where(pct_low_match,
                                                                   self.analysis_ds[varname],
                                                                   self.analysis_ds["WSE_10pct"])
            else:
                self.high_low_ds["NextHighestElevation"] = xr.where(pct_high_match,
                                                                    self.analysis_ds[varname],
                                                                    self.high_low_ds["NextHighestElevation"])
                self.high_low_ds["NextLowestElevation"] = xr.where(pct_low_match,
                                                                   self.analysis_ds[varname],
                                                                   self.high_low_ds["NextLowestElevation"])

        # Fill some nodata
        high_fill = xr.where(self.high_low_ds["NextHighestElevation"].isnull() & self.valid_loc, 1, 0)
        low_fill = xr.where(self.high_low_ds["NextLowestElevation"].isnull() & self.valid_loc, 1, 0)
        test_export_array(high_fill, self.output_subfolders[4], 598)
        test_export_array(low_fill, self.output_subfolders[4], 599)
        self.high_low_ds["NextHighestElevation"] = xr.where(high_fill == 1,
                                                            self.analysis_ds["WSE_0_2pct"],
                                                            self.high_low_ds["NextHighestElevation"])
        self.high_low_ds["NextLowestElevation"] = xr.where(low_fill == 1,
                                                           self.analysis_ds["WSE_10pct"],
                                                           self.high_low_ds["NextLowestElevation"])

        self.high_low_ds = self.high_low_ds.chunk({"x": 2048, "y": 2048})

        # Export the next highest and lowest elevations
        oupath = os.path.join(self.output_subfolders[2], "NextHighestElevations.tif")
        self.high_low_ds["NextHighestElevation"] = self.high_low_ds["NextHighestElevation"].rio.write_nodata(-9999,
                                                                                                             inplace=True,
                                                                                                             encoded=False)
        export_raster(self.high_low_ds["NextHighestElevation"], oupath)
        self.high_low_ds["NextHighestElevation"] = self.high_low_ds["NextHighestElevation"].where(
            self.high_low_ds["NextHighestElevation"] != -9999)

        oupath = os.path.join(self.output_subfolders[2], "NextLowestElevations.tif")
        self.high_low_ds["NextLowestElevation"] = self.high_low_ds["NextLowestElevation"].rio.write_nodata(-9999,
                                                                                                           inplace=True,
                                                                                                           encoded=False)
        export_raster(self.high_low_ds["NextLowestElevation"], oupath)
        self.high_low_ds["NextLowestElevation"] = self.high_low_ds["NextLowestElevation"].where(
            self.high_low_ds["NextLowestElevation"] != -9999)

    def compute_perc_annual_chance(self):
        """
        Compute the percent chance
        """

        # Drop the WSE rasters
        to_drop = [v for v in self.analysis_ds.data_vars.keys() if "WSE" in v]
        print(f"Variables to drop: {to_drop}\nStarting Percent-Annual-Chance Calculation...")
        self.analysis_ds = self.analysis_ds.drop_vars(to_drop)
        ground_dem = self.analysis_ds["Ground"].copy(deep=True).chunk({"x": 2048, "y": 2048})
        del self.analysis_ds

        # Set up the variables
        y1 = self.high_low_ds["NextLowestElevation"]
        y2 = ground_dem
        y3 = self.high_low_ds["NextHighestElevation"]
        x1 = self.high_low_ds["NextLowestPct"]
        x3 = self.high_low_ds["NextHighestPct"]

        tolerance = 1e-3
        denom_diff = abs(y3 - y1)
        denom_bad = xr.where(denom_diff < tolerance, 1, 0)
        test_export_array(denom_bad, self.output_subfolders[4], 646, 1)

        # Compute the percent annual chance
        x2 = 10 ** ((y2 - y1) * (np.log10(x3) - np.log10(x1)) / (y3 - y1) + np.log10(x1))
        x2 = x2.astype(np.float32)
        x2.rio.write_nodata(-9999, inplace=True)
        x2 = x2.chunk({"x": 2048, "y": 2048})

        # Export the PAC
        print(f"Exporting PAC")
        pac_path = os.path.join(self.output_subfolders[3], "PAC.tif")
        export_raster(x2, pac_path)

        return x2

    def compute_n_year_chance(self, perc_annual_chance, n_years):
        """
        Compute the 30-year chance
        """
        # Compute the n-year chance
        perc_annual_chance = perc_annual_chance.where(perc_annual_chance != perc_annual_chance.rio.nodata)
        n_year_chance = 1 - (1 - perc_annual_chance) ** n_years
        n_year_chance.rio.write_nodata(-9999, inplace=True)

        # Export the n-year chance
        n_year_path = os.path.join(self.output_subfolders[3], f"{n_years}_year_chance.tif")
        export_raster(n_year_chance, n_year_path)

        return n_year_chance

    def process_perc_chance(self):
        """
        Process the percent chance
        """
        # Timer
        t1_start = process_time()
        tb_times = []

        # Create the output folders
        os.makedirs(self.output_path, exist_ok=True)
        for f in self.output_subfolders:
            os.makedirs(f, exist_ok=True)

        print(f"Value Raster, {self.vspecs.__repr__()}")

        # Open the valid raster
        validity = self.open_valid_xarray()
        tb_times.append(process_time())

        # Open the DEM
        self.open_dem_xarray(validity)
        tb_times.append(process_time())

        # Open the WSE rasters
        self.open_wse_xarrays()
        tb_times.append(process_time())
        for t in tb_times:
            print(f"Time: {round(t - t1_start, 1)} seconds")

        # Fill the WSE rasters with DEM
        # self.fill_wse_with_dem()
        # tb_times.append(process_time())

        # Create pct start grid
        self.create_pct_start_grid()
        tb_times.append(process_time())

        # Compute the percent chance grids
        self.compute_next_high_next_low()
        tb_times.append(process_time())

        # Compute elevations from perc chance grids
        self.compute_next_high_next_low_elev()
        tb_times.append(process_time())
        print(f"Computed Percent Chances")

        # print some times
        for t in tb_times:
            print(f"Time: {round(t - t1_start, 1)} seconds")

        # Compute the percent annual chance
        pac = self.compute_perc_annual_chance()

        # Calculate 30-year chance
        n_year_chance = self.compute_n_year_chance(pac, 30)
        tb_times.append(process_time())

        t1_stop = process_time()

        print(f" Processed percent chance in {t1_stop - t1_start} seconds")
        print(f" Time breakdown: ")
        for i, elapsed in enumerate(tb_times):
            print(f"  {i}: {elapsed}")


if __name__ == "__main__":
    input_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance"
    output_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance_OUT"
    dem_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance\ground_dem_CY.tif"

    pcg = PercChanceGrids(input_folder, output_folder, dem_path)
    pcg.process_perc_chance()
