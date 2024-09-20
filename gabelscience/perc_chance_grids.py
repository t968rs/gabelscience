import threading
import numpy as np
from math import log10
import rioxarray as rioxr
import os
import typing as T
from sparse import COO
import xarray as xr
from src.specs.raster_specs import create_raster_specs_from_path
import logging
from src.d01_processing.export_raster import export_raster, test_export_array
from time import process_time
import dask.config
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


def drop_attributes(array):
    """
    Drop attributes from an xarray
    """
    return array


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


def get_return_name(return_number):
    """
    Get the return name
    """
    if return_number < 0:
        return_number = [k for k, v in RETURNS.items() if v == return_number][0]
    return [k for k, v in RETURN_NAMES.items() if v == return_number][0]


def update_stats_atts(array):
    """
    Update the stats attributes
    """
    stats_mask = array.where(array != array.rio.nodata)
    stats = {"STATISTICS_STDDEV": np.round(stats_mask.std().values, 2),
             "STATISTICS_MEAN": np.round(stats_mask.mean().values, 2),
             "STATISTICS_MAXIMUM": np.round(stats_mask.max().values, 2),
             "STATISTICS_COUNT": np.round(stats_mask.count().values, 2),
             "STATISTICS_MINIMUM": np.round(stats_mask.min().values, 2),}
    array.assign_attrs(stats)
    return array


RETURNS = {10: 0.0002, 9: 0.01, 8: 0.02,
           7: 0.04, 6: 0.10}

RETORD_LOOKUP = {v: k for k, v in RETURNS.items()}

RETURN_NAMES = {"_0_2pct": 10, "_01pct": 9, "_02pct": 8,
                "_04pct": 7, "_10pct": 6}

OUTPUT_SUBFOLDERS = ["00_DEM_Masked", "01_WSE_Filled", "02_WSE_Nearest_Ground", "03_PAC", "__INTMD__"]


class PercChanceGrids:
    def __init__(self, input_path, output_path, dem):
        self.input_path = input_path
        self.output_path = output_path
        self.input_dem_path = dem

        self.return_paths = {}
        self.xarrays_ready = {}
        self.dem = xr.DataArray

        self._init_xarrays_ready()
        # Find the WSE rasters
        self._find_wse_rasters()
        self._value_raster_specs = create_raster_specs_from_path(self.return_paths[10])
        self._set_configs()

    @property
    def vspecs(self):
        return self._value_raster_specs

    @property
    def output_subfolders(self):

        return self._subfolder_paths()

    def _subfolder_paths(self):
        return [os.path.join(self.output_path, f) for f in OUTPUT_SUBFOLDERS]

    @property
    def dem_path(self):
        masked_dem_path = os.path.join(self.output_subfolders[0], "dem_masked.tif")
        if os.path.exists(masked_dem_path):
            return masked_dem_path
        else:
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

    def _init_xarrays_ready(self):
        """
        Initialize the xarrays ready
        """
        self.xarrays_ready = {k: None for k, v in RETURN_NAMES.items()}

    def add_to_ready_xarrays(self, key: str, value: xr.DataArray):
        """
        Hold the xarrays ready
        """
        if key not in RETURN_NAMES.keys():
            raise ValueError(f"Key {key} not in {RETURN_NAMES.keys()}")
        self.xarrays_ready[key] = value

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
        print(f"\nValid xarray: \n{valid_xarray.attrs}\n")
        export_raster(valid_xarray, os.path.join(self.output_subfolders[1], "WSE_0_2pct.tif"))

        # Add to the ready xarrays
        self.add_to_ready_xarrays("_0_2pct", valid_xarray)

    def open_dem_xarray(self):
        # Open DEM and clip to valid bounds
        print(f"DEM path: {self.dem_path}")
        dem = rioxr.open_rasterio(self.dem_path, chunks={"x": 2048, "y": 2048}).rename("DEM")
        atts = {"crs": self.vspecs.crs, "BandName": "DEM",
                "long_name": "Digital Elevation Model", "DataType": "Elevation"}
        dem = dem.assign_attrs(atts)
        demcrs = dem.rio.crs
        print(f"DEM CRS: {demcrs}")
        dem = dem.rio.clip_box(*self.vspecs.bounds).chunk({"x": 2048, "y": 2048})

        # align the DEM and valid raster
        print(f'Aligning DEM and valid raster')
        validarray = self.xarrays_ready["_0_2pct"]
        dem.reindex_like(validarray, method="nearest", tolerance=1.5)
        # test_export_array(dem, self.output_subfolders[4], 243)
        _, dem = xr.align(self.xarrays_ready["_0_2pct"], dem, join="left")

        # Mask the DEM by valid raster
        dem = dem.where(validarray != validarray.rio.nodata).chunk({"x": 2048, "y": 2048})
        dem = dem.rio.write_nodata(-9999, inplace=True, encoded=False)

        # Round the DEM to 2 decimal places
        dem = dem.round(2)
        dem.rio.write_nodata(-9999, inplace=True, encoded=False)
        dem = update_stats_atts(dem)

        # Export the masked DEM
        oupath = os.path.join(self.output_subfolders[0], "DEM_masked.tif")
        if not os.path.exists(oupath):
            export_raster(dem, oupath)

        print(f"Masked DEM:")
        for k, v in dem.attrs.items():
            print(f"\t{k}: \t\t{v}")
        self.dem = dem

    def open_wse_xarrays(self):
        """
        Open the WSE xarrays
        """
        opened = {}
        for key, path in self.return_paths.items():
            if key == 10:
                continue

            wse = rioxr.open_rasterio(path, chunks={"x": 2048, "y": 2048}).rename(f"WSE{get_return_name(key)}")
            wse.rio.write_crs(self.vspecs.crs, inplace=True)
            attrs = {"crs": self.vspecs.crs, "BandName": f"WSE{get_return_name(key)}",
                     "long_name": f"Water Surface Elevation {RETURNS[key] * 100}%",
                     "DataType": "Elevation"}
            wse = wse.assign_attrs(attrs)

            # Re-index the WSE
            wse = wse.reindex_like(self.xarrays_ready["_0_2pct"], method="nearest", tolerance=1.5)
            # print(f"\nWSE{key} xarray: \n{wse}")
            opened[key] = wse

        # Align the WSE to the DEM
        aligned_wse_list = align_xarrays(self.xarrays_ready["_0_2pct"], opened)[1:]
        returns_aligned = [a.name.split("WSE")[1] for a in aligned_wse_list if "WSE" in a.name and
                           a.name.split("WSE")[1] in RETURN_NAMES.keys()]
        aligned_wse_dict = {k: v for k, v in zip(returns_aligned, aligned_wse_list)}
        print(f"\nAligned WSE: {list(aligned_wse_dict.keys())}")
        print(f"Array Names: {[v.name for v in aligned_wse_dict.values()]}\n")

        for key, wse in aligned_wse_dict.items():
            print(f'\tnodata: {wse.rio.nodata}\n\tencoded: {wse.rio.encoded_nodata}')
            # Round the WSE to 1 decimal places
            wse = wse.round(1)
            # Fill the wse with DEM where it is null
            wse_gaps = wse.where(wse == wse.rio.nodata)
            print(f"\nFilling WSE{key} where it is null")
            wse = xr.where(wse_gaps, self.dem, wse).chunk({"x": 2048, "y": 2048})
            print(f'\tnodata: {wse.rio.nodata}\n\tencoded: {wse.rio.encoded_nodata}')
            wse.rio.write_nodata(-9999, inplace=True, encoded=False)
            print(f'\tnodata: {wse.rio.nodata}\n\tencoded: {wse.rio.encoded_nodata}')
            print(f" Filled WSE{key}: \n{wse}")
            self.add_to_ready_xarrays(f"{key}", wse)

            # Export the filled WSE
            oupath = os.path.join(self.output_subfolders[1], f"WSE{key}_filled.tif")
            export_raster(wse, oupath)

    def compute_next_high_next_low(self):
        """
        Compute the percent annual chance
        """
        # Set up validity and dem arrays
        validity, dem = self.xarrays_ready["_0_2pct"], self.dem
        valid_loc = validity != validity.rio.nodata

        # Set up the next highest and lowest array holders
        next_highest_xarray = (xr.zeros_like(validity, chunks=validity.chunks).where(valid_loc)
                               .rename("NextHighestElevation"))
        next_lowest_xarray = (xr.zeros_like(validity, chunks=validity.chunks).where(valid_loc)
                              .rename("NextLowestElevation"))
        nearest_to_dem = (xr.zeros_like(validity, chunks=validity.chunks).where(valid_loc).rename("NearestToDEM"))

        # Loop through the xarrays
        for i, key, xarray in enumerate(self.xarrays_ready.items()):
            diff = xarray - self.dem

            # Nearest to DEM
            if i == 0:
                nearest_to_dem = xr.where(valid_loc & xarray != xarray.rio.nodata, key, nearest_to_dem)
            else:
                nearest_to_dem = xr.where(valid_loc & (abs(diff) < nearest_to_dem), key, nearest_to_dem)

            # High logic
            high_not_zero = next_highest_xarray != 0
            higher_than_dem = diff > 0

            next_highest_1 = xr.where(higher_than_dem & high_not_zero & (xarray < next_highest_xarray), True, False)
            next_highest_2 = xr.where(higher_than_dem & ~high_not_zero, True, False)

            # High calculation
            next_highest_xarray = xr.where(next_highest_1 | next_highest_2, xarray, next_highest_xarray)

            # Low logic
            low_not_zero = next_lowest_xarray != 0
            lower_than_dem = diff < 0

            next_lowest_1 = xr.where(lower_than_dem & (low_not_zero & xarray > next_lowest_xarray), True, False)
            next_lowest_2 = xr.where(lower_than_dem & ~low_not_zero, True, False)

            # Low calculation
            next_lowest_xarray = xr.where(next_lowest_1 | next_lowest_2, xarray, next_lowest_xarray)

        # Chunk them
        next_highest_xarray = next_highest_xarray.chunk({"x": 2048, "y": 2048})
        next_lowest_xarray = next_lowest_xarray.chunk({"x": 2048, "y": 2048})
        nearest_to_dem = nearest_to_dem.chunk({"x": 2048, "y": 2048})

        # Create near-ground percentages
        for n, pct in RETURNS:
            nearest_to_dem = nearest_to_dem.where(nearest_to_dem == n, pct)

        # Export next highest and lowest rasters
        print(f" Exporting next highest and lowest rasters")
        highpath = os.path.join(self.output_subfolders[2], "next_highest.tif")
        export_raster(next_highest_xarray, highpath)
        # Export lowest
        lowpath = os.path.join(self.output_subfolders[2], "next_lowest.tif")
        export_raster(next_lowest_xarray, lowpath)
        # Export nearest to DEM
        ntdpath = os.path.join(self.output_subfolders[2], "nearest_to_dem.tif")
        export_raster(nearest_to_dem, ntdpath)

        return next_highest_xarray, next_lowest_xarray, nearest_to_dem

    def compute_perc_chance(self, nh, nl, ng):
        """
        Compute the percent chance
        """

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
            pcpath = os.path.join(self.output_subfolders[3], f"perc{get_return_name(n)}_chance.tif")
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
        self.open_valid_xarray()
        tb_times.append(process_time())

        # Open the DEM
        self.open_dem_xarray()
        tb_times.append(process_time())

        # Open the WSE rasters
        self.open_wse_xarrays()
        tb_times.append(process_time())

        # Compute the percent chance
        next_high, next_low, nearest_grnd = self.compute_next_high_next_low()
        tb_times.append(process_time())

        # Compute the percent chance
        pct_chances = self.compute_perc_chance(next_high, next_low, nearest_grnd)
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
    input_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Perc_Chance_IN"
    output_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Perc_Chance_OUT"
    dem_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\DEM\ground_dem_CY.tif"

    pcg = PercChanceGrids(input_folder, output_folder, dem_path)
    pcg.process_perc_chance()
