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


def write_xarray_to_raster(array, path, src):
    """
    Write the array to a raster
    """
    array.to_raster(path, driver="GTiff", windowed=True, bigtiff="yes", compress="LZW", dtype="float32", crs=src.crs, )


def find_rasters(path):
    """
    Find the rasters in the given path
    """
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".tif"):
                yield os.path.join(root, file)


def get_lower_return(current_return):
    """
        Get the higher and lower grids for the given returns
        """
    return_number = [k for k, v in RETURNS.items() if v == current_return][0]

    # Get lower
    if not return_number == 6:
        lower_number = return_number - 1
    else:
        lower_number = 6

    return RETURNS.get(lower_number, return_number), return_number


def sparse_an_xarray(array: xr.DataArray):
    """
    Convert an xarray to a sparse array
    """
    return xr.apply_ufunc(COO, array, dask="parallelized", output_dtypes=[array.dtype])


def align_xarrays(left_array,
                  other_arrays: T.Union[T.List, T.Dict, T.Tuple, xr.DataArray]):
    """
    Align the xarrays
    """
    if isinstance(other_arrays, dict):
        array_list = [left_array] + list(other_arrays.values())
    elif isinstance(other_arrays, list) or isinstance(other_arrays, tuple):
        array_list = [left_array] + list(other_arrays)
    else:
        array_list = [left_array, other_arrays]
    return xr.align(*array_list, join="left")


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


RETURNS = {10: 0.0002, 9: 0.01, 8: 0.02,
           7: 0.04, 6: 0.10}

RETURN_LOOKUP = {v: k for k, v in RETURNS.items()}

RETURN_NAMES = {"_0_2pct": 10, "_01pct": 9, "_02pct": 8,
                "_04pct": 7, "_10pct": 6}

RETURN_NAME_LOOKUP = {v: k for k, v in RETURN_NAMES.items()}

OUTPUT_SUBFOLDERS = ["00_DEM_Masked", "01_WSE_Filled", "02_WSE_Nearest_Ground", "03_PAC", "__INTMD__"]


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

    @staticmethod
    def compute_perc_variables(current_return):
        """
        Compute the variables for the percent annual chance
        """
        # Store return and orders
        lower_return, current_order = get_lower_return(current_return)
        if current_order == 10:
            higher_return = current_return
        else:
            higher_return = RETURNS.get(current_order + 1)

        p2, p1 = log10(higher_return) - log10(lower_return), log10(lower_return)

        return p2, p1

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

        # Round it to 1 decimal places
        valid_xarray = valid_xarray.round(1)

        # Update the stats attributes
        valid_xarray = update_stats_atts(valid_xarray)

        # Export the valid raster
        # print(f"\nValid xarray: \n{valid_xarray.attrs}\n")
        export_raster(valid_xarray, os.path.join(self.output_subfolders[1], "WSE_0_2pct.tif"))
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
        dem = dem.where(varray != varray.rio.nodata)
        dem = dem.rio.write_nodata(-9999, inplace=True, encoded=False)
        print(f"Masked DEM SHape: \n{dem.shape}")
        print(f"Other Shape: \n{varray.shape}")

        # Round the DEM to 2 decimal places
        dem = dem.round(2)
        dem.rio.write_nodata(-9999, inplace=True, encoded=False)
        dem = update_stats_atts(dem)

        # Export the masked DEM
        oupath = os.path.join(self.output_subfolders[0], "DEM_masked.tif")
        if not os.path.exists(oupath):
            export_raster(dem, oupath)

        # Print bounds for storage
        print(f"Masked DEM:")
        # for k, v in dem.attrs.items():
        #     print(f"\t{k}: \t\t{v}")
        print(f"DEM Shape: \n{dem.shape}")
        bounds = dem.rio.bounds()
        bbox_to_gdf(bounds, self.vspecs.crs, "dem_bounds", self.output_subfolders[4])

        # Convert to dataset
        self.analysis_ds = dem.to_dataset(dim="band")
        dem_var_name = list(self.analysis_ds.data_vars)[0]  # Get the current variable name
        self.analysis_ds = self.analysis_ds.rename_vars({dem_var_name: "Ground"})
        # Drop the 'band' dimension if it exists
        if 'band' in varray.dims:
            varray = varray.isel(band=0, drop=True)
        self.analysis_ds["WSE_0_2pct"] = varray
        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})
        for var, _ in self.analysis_ds.data_vars.items():
            print(f"\nVariable: {var}, {self.analysis_ds[var].nbytes / 1e9} GB")
            self.analysis_ds[var] = self.analysis_ds[var].rio.write_nodata(-9999, inplace=True, encoded=False)
            if var == "Ground":
                self.add_specs(f"OPEN_{var}", array=self.analysis_ds[var])
            self.add_specs(f"OPEN_{var}", array=self.analysis_ds[var])
            test_export_array(self.analysis_ds[var], self.output_subfolders[4], 297)

        # Delete the antecedent arrays to free up resources
        del dem, varray

        print(f"\nDS: \n{self.analysis_ds}")

    def open_wse_xarrays(self):
        """
        Open the WSE xarrays
        """
        for key, path in self.return_paths.items():
            if key == 10:
                continue

            dsw = rioxr.open_rasterio(path, band_as_variable=True, chunks={"x": 2048, "y": 2048})
            new_varname = f"WSE{RETURN_NAME_LOOKUP.get(key, 'WSE_UNK')}"
            dsw = dsw.rename_vars({"band_1": new_varname})
            print(f"\nOpened {dsw.nbytes / 1e9} GB")
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

            # Re-index the WSE
            no_data = dsw[new_varname].attrs["_FillValue"] if "_FillValue" in dsw[new_varname].attrs else -9999
            if len(dsw.data_vars) > 1:
                raise ValueError("More than one variable in the dataset")
            dsw = dsw.rio.clip(self.valid_gdf.geometry)
            dsw = dsw.reindex_like(self.analysis_ds["Ground"], method="nearest", tolerance=1.5)
            if dsw[new_varname].shape != self.analysis_ds["Ground"].shape:
                raise ValueError(
                    f"Shapes do not match: {dsw[new_varname].shape} and {self.analysis_ds['Ground'].shape}")
            dsw = dsw.rio.pad_box(*self.all_specs["OPEN_Ground"].bounds)
            dsw[new_varname] = dsw[new_varname].rio.write_nodata(no_data, inplace=True, encoded=False)
            dsw.rio.write_crs(self.vspecs.crs, inplace=True)

            # Round the WSE to 1 decimal places
            dsw = dsw.round(1)
            test_export_array(dsw[new_varname], self.output_subfolders[4], 335)
            self.analysis_ds = xr.combine_by_coords([self.analysis_ds, dsw],
                                                    join="outer", combine_attrs="drop_conflicts")
        print(f"\nDS: \n{self.analysis_ds}")
        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})
        self.analysis_ds = drop_atts(self.analysis_ds, ["BandName", "long_name"])
        # Handle some nodata
        for varname, darr in self.analysis_ds.data_vars.items():
            print(f"\n Variable: {varname}, {self.analysis_ds[varname].nbytes / 1e9} GB")
            # darr.rio.write_nodata(-9999, inplace=True, encoded=False)
            test_export_array(self.analysis_ds[varname], self.output_subfolders[4], 346)

        print(f"\nDS: \n{self.analysis_ds}")

    def fill_wse_with_dem(self):
        filler = self.analysis_ds["Ground"]
        test_export_array(self.analysis_ds, self.output_subfolders[4], 348)
        for varname, dv in self.analysis_ds.data_vars.items():
            print(f"\n Variable: {varname}")

            if "WSE" in varname:
                darr = self.analysis_ds[varname]
                test_export_array(darr, self.output_subfolders[4], 356)
                print(f"\nFilling {varname} where it is null")
                print(f"{darr.name} : {darr.shape}")
                where_test = xr.where((darr > 0), 1, 0)
                where_test.rio.write_nodata(-9999, inplace=True, encoded=False)
                test_export_array(where_test, self.output_subfolders[4], 361)
                print(f'\tnodata: {darr.rio.nodata}\n\tencoded: {darr.rio.encoded_nodata}')

                # darr_fills = darr.where(darr == darr.rio.nodata, filler)
                # test_export_array(darr_fills, self.output_subfolders[4], 354)
                # darr_filled = darr.where(darr != darr.rio.nodata) + darr_fills
                # test_export_array(darr_filled, self.output_subfolders[4], 356)
                # print(f"\tFilled array min: {darr_fills.min().values}, max: {darr_fills.max().values}")
                # darr_filled.rio.write_nodata(-9999, inplace=True, encoded=False)
                # print(f'\tnodata: {darr_filled.rio.nodata}\n\tencoded: {darr_filled.rio.encoded_nodata}')
                # print(f" Filled {varname}: \n{darr_filled}")
                # self.analysis_ds[varname] = darr_filled

                # Export the filled WSE
                oupath = os.path.join(self.output_subfolders[1], f"{varname}_filled.tif")
                outarr = self.analysis_ds[varname]
                outarr.rio.to_raster(oupath, tiled=True, lock=threading.Lock(), compress='LZW',
                                     windowed=True, bigtiff="YES")

        self.analysis_ds = self.analysis_ds.chunk({"x": 2048, "y": 2048})

    def compute_next_high_next_low(self):
        """
        Compute the percent annual chance
        """
        # Set up validity and dem arrays
        dem = self.analysis_ds["Ground"]
        valid_loc = dem != dem.rio.nodata

        # Set up the next highest and lowest array holders
        self.analysis_ds["NextHighestElevation"] = xr.zeros_like(dem, chunks=dem.chunks).where(valid_loc)
        self.analysis_ds["NextLowestElevation"] = xr.zeros_like(dem, chunks=dem.chunks).where(valid_loc)
        self.analysis_ds["NearestToDEM"] = xr.zeros_like(dem, chunks=dem.chunks).where(valid_loc)

        # Loop through the xarrays
        for i, (vname, xarray) in enumerate(self.analysis_ds.data_vars.items()):
            if "WSE" not in vname:
                continue
            diff = xarray - dem

            # Nearest to DEM
            if i == 0:
                self.analysis_ds["NearestToDEM"] = xr.where(
                    valid_loc & xarray != xarray.rio.nodata, vname, self.analysis_ds["NearestToDEM"])
            else:
                self.analysis_ds["NearestToDEM"] = xr.where(
                    valid_loc & (abs(diff) < self.analysis_ds["NearestToDEM"]), vname, self.analysis_ds["NearestToDEM"])
            self.analysis_ds["NearestToDEM"] = self.analysis_ds["NearestToDEM"].rio.write_nodata(-9999, inplace=True)

            # High logic
            high_not_zero = self.analysis_ds["NextHighestElevation"] != 0
            higher_than_dem = diff > 0

            next_highest_1 = xr.where(
                higher_than_dem & high_not_zero & (xarray < self.analysis_ds["NextHighestElevation"]), True, False)
            next_highest_2 = xr.where(higher_than_dem & ~high_not_zero, True, False)

            # High calculation
            self.analysis_ds["NextHighestElevation"] = xr.where(
                next_highest_1 | next_highest_2, xarray, self.analysis_ds["NextHighestElevation"])
            self.analysis_ds["NextHighestElevation"] = (self.analysis_ds["NextHighestElevation"]
                                                        .rio.write_nodata(-9999, inplace=True))

            # Low logic
            low_not_zero = (self.analysis_ds["NextLowestElevation"] != 0).astype(bool)
            lower_than_dem = (diff < 0).astype(bool)
            x_morethan_holder = (xarray > self.analysis_ds["NextLowestElevation"]).astype(bool)

            next_lowest_1 = xr.where(
                lower_than_dem & (low_not_zero & x_morethan_holder), True, False)
            next_lowest_2 = xr.where(lower_than_dem & ~low_not_zero, True, False)

            # Low calculation
            self.analysis_ds["NextLowestElevation"] = xr.where(
                next_lowest_1 | next_lowest_2, xarray, self.analysis_ds["NextLowestElevation"])
            self.analysis_ds["NextLowestElevation"] = (self.analysis_ds["NextLowestElevation"]
                                                       .rio.write_nodata(-9999, inplace=True))

        # Chunk them
        self.analysis_ds.chunk({"x": 2048, "y": 2048})

        # Create near-ground percentages
        print(f"Creating near-ground percentages")
        print(f"For: {[str(v * 100) + "%," for v in RETURNS.values()]} returns")
        for n, pct in RETURNS.items():
            self.analysis_ds["NearestToDEM"] = self.analysis_ds["NearestToDEM"].where(
                self.analysis_ds["NearestToDEM"] == n, pct)
        self.analysis_ds["NearestToDEM"] = self.analysis_ds["NearestToDEM"].rio.write_nodata(-9999, inplace=True)

        # Export nearest to DEM
        ntdpath = os.path.join(self.output_subfolders[2], "nearest_to_dem.tif")
        export_raster(self.analysis_ds["NearestToDEM"], ntdpath)

        print(f" Exporting next highest and lowest rasters")
        # Export next highest and lowest rasters
        highpath = os.path.join(self.output_subfolders[2], "next_highest.tif")
        export_raster(self.analysis_ds["NextHighestElevation"], highpath)
        # Export lowest
        lowpath = os.path.join(self.output_subfolders[2], "next_lowest.tif")
        export_raster(self.analysis_ds["NextLowestElevation"], lowpath)

    def compute_perc_chance(self):
        """
        Compute the percent chance
        """
        nl = self.analysis_ds["NextLowestElevation"]
        nh = self.analysis_ds["NextHighestElevation"]
        ng = self.analysis_ds["NearestToDEM"]
        perc_chances = {}

        y2 = self.dem
        for n, pct in RETURNS.items():
            print(f"\nComputing percent chance for {n}")
            up_one = RETURNS.get(n + 1, pct)
            down_one = RETURNS.get(n - 1, pct)
            # Compute percentages
            p2, p3 = log10(up_one) - log10(down_one), log10(down_one)

            # Compute the rasters
            y1 = nl.where((nl != nl.rio.nodata) & (ng == down_one))
            y3 = nh.where((nh != nh.rio.nodata) & (ng == up_one))

            # Calculate the percent chance for this return period
            x2 = 10 ** (((y2 - y1) * p2) / (y3 - y1) + p3)
            x2 = x2.where((x2 != x2.rio.nodata) & (x2 > 0), 0).rename(f"PAC_{pct * 100}pct")
            x2 = x2.round(2)
            perc_chances[n] = x2
            print(f" Percent Chance {n}: \n{x2}")

            # Export the percent chance
            print(f" Exporting percent chance")
            pcpath = os.path.join(self.output_subfolders[3], f"perc{RETURN_NAME_LOOKUP.get(n, None)}_chance.tif")
            export_raster(x2, pcpath)

        return perc_chances

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
        self.fill_wse_with_dem()
        tb_times.append(process_time())

        # Compute the percent chance
        self.compute_next_high_next_low()
        tb_times.append(process_time())

        # Compute the percent chance
        pct_chances = self.compute_perc_chance()
        tb_times.append(process_time())
        print(f" Percent Chances: {pct_chances.keys()}")
        ds = xr.combine_by_coords(list(pct_chances.values()), fill_value=0.0, join="outer",
                                  combine_attrs="drop_conflicts")
        print(f"\n Combined DS: \n{ds}")

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
