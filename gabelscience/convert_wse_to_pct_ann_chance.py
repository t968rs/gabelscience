import os
import numpy as np
import dask.config
import dask.array as da
import rasterio
from rasterio.mask import mask
import rasterio.windows
from rasterio.transform import from_bounds
import logging
from src.specs.raster_specs import create_raster_specs_from_path
from src.d00_utils.files_finder import get_raster_list
from src.d01_processing.export_raster import *
from time import process_time



def pad_array(array, target_shape):
    """Pad the array to the target shape with NaNs."""
    pad_width = [(0, max(0, target_shape[i] - array.shape[i])) for i in range(array.ndim)]
    return da.pad(array, pad_width, mode='constant', constant_values=np.nan)

def get_union_shape(arrays):
    """Get the union shape (maximum dimensions) from a list of arrays."""
    max_shape = [0] * arrays[0].ndim
    for array in arrays:
        for i in range(array.ndim):
            max_shape[i] = max(max_shape[i], array.shape[i])
    return tuple(max_shape)

def load_raster_as_dask_array(raster_path, fill_value=-9999):
    path_ordering = None
    with rasterio.open(raster_path) as src:
        data = src.read(1)
        profile = src.profile
        profile.update(bounds=src.bounds)
        filename = os.path.basename(raster_path)
        name = os.path.splitext(filename)[0]
        for key, value in RETURN_NAMES.items():
            if key in raster_path:
                path_ordering = value
                break
        dask_data = da.from_array(data, chunks=(2048, 2048), name=name).astype(np.float32)
        dask_data = da.where(dask_data == fill_value, np.nan, dask_data)
    return dask_data, profile, path_ordering


def mosaic_rasters(raster_paths, reduction='max'):
    # Load all rasters as dask arrays
    print(f"Starting Mosaic...")
    dask_arrays = []
    profiles = {}
    for path in raster_paths:

        dask_data, profile, path_order = load_raster_as_dask_array(path)
        dask_arrays.append((dask_data, path_order))
        profiles[path] = profile

    dask_arrays.sort(key=lambda x: x[1])
    # dask_arrays = [(dask_array2, 6), (dask_array3, 8), (dask_array1, 10)]

    # Extracting the sorted dask arrays
    sorted_arrays = [arr[0] for arr in dask_arrays]
    # sorted_arrays = [dask_array2, dask_array3, dask_array1]

    # Determine the maximum shape
    max_shape = tuple(max(arr.shape[i] for arr in sorted_arrays) for i in range(sorted_arrays[0].ndim))

    # Pad arrays to the maximum shape
    padded_arrays = [pad_array(arr, max_shape) for arr in sorted_arrays]

    # Stack the arrays along a new axis
    stacked_array = da.stack(padded_arrays, axis=0)

    # Apply the reduction operation
    if reduction == 'max':
        mosaic = da.nanmax(stacked_array, axis=0)
    elif reduction == 'mean':
        mosaic = da.nanmean(stacked_array, axis=0)
    elif reduction == 'first':
        mosaic = stacked_array[0]
    else:
        raise ValueError(f"Unsupported reduction operation: {reduction}")

    # Update profile for the mosaic
    base_profile = profiles[raster_paths[0]]
    base_bounds = base_profile['bounds']
    mosaic_transform  = from_bounds(*base_bounds, mosaic.shape[1], mosaic.shape[0])
    mosaic_profile = base_profile.copy()
    mosaic_profile.update(height=mosaic.shape[0], width=mosaic.shape[1])
    mosaic_profile.update(transform=mosaic_transform)

    return mosaic, mosaic_profile


def map_to_next_(arr, lookup):
    """Map values in arr to their next-lowest or next-highest values using the lookup dictionary."""
    vectorized_lookup = np.vectorize(lambda x: lookup.get(x, x))
    return vectorized_lookup(arr)

def get_next_pct_(pct_array, higher=False):
    """Apply the next-lowest mapping to the dask array."""
    next_lowest_lookup = {RETURNS[k]: RETURNS.get(k - 1, RETURNS[k]) for k in sorted(RETURNS.keys(), reverse=True)}
    next_higher_lookup = {RETURNS[k]: RETURNS.get(k - 1, RETURNS[k]) for k in sorted(RETURNS.keys(), reverse=True)}
    if not higher:
        return da.map_blocks(map_to_next_, pct_array, lookup=next_lowest_lookup, dtype=pct_array.dtype)
    else:
        return da.map_blocks(map_to_next_, pct_array, lookup=next_higher_lookup, dtype=pct_array.dtype)



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


RETURNS = {10: 0.002, 9: 0.010, 8: 0.020,
           7: 0.040, 6: 0.100}

RETURN_LOOKUP = {v: k for k, v in RETURNS.items()}

RETURN_NAMES = {"_0_2pct": 10, "_01pct": 9, "_02pct": 8,
                "_04pct": 7, "_10pct": 6}

RETURN_NAME_LOOKUP = {v: k for k, v in RETURN_NAMES.items()}



class WSEtoPercentAnnualChanceReturn:

    def __init__(self, in_path, out_path):
        self.input_path = in_path
        self.output_path = out_path

        self.return_paths = {}
        self.xarrays_ready = {}
        self.valid_gdf = None
        self.dem = None
        self.empty_valid = None
        self.analysis_ds = {}
        self.high_low_ds = {}
        self.valid_loc = None

        # Find the WSE rasters
        self._find_wse_rasters()
        self._value_raster_specs = create_raster_specs_from_path(self.return_paths[10])
        self.valid_profile = {}
        self._all_raster_specs = {}
        self._set_configs()

        logger.info(f"\n\nNEW RUN\n\n")
        logger.info(f"Value Raster Specs: {self.vspecs}")

    @staticmethod
    def _set_configs():
        dask.config.set(**{'array.slicing.split_large_chunks': False})

    @property
    def vspecs(self):
        return self._value_raster_specs

    @property
    def all_specs(self):
        if not self._all_raster_specs:
            self._init_all_raster_specs()
        return self._all_raster_specs

    def _init_all_raster_specs(self):
        for key, path in self.return_paths.items():
            self._all_raster_specs[f"IN_{key}"] = create_raster_specs_from_path(path)

    def _find_wse_rasters(self):
        for path in get_raster_list(self.input_path, recursive=False):
            if "WSE" not in path:
                continue
            else:
                found, unfound = [], []
                for key, value in RETURN_NAMES.items():
                    if key in path:
                        self.return_paths[value] = path
                        found.append(key)
                        break
                unfound = [key for key in RETURN_NAMES.keys() if key not in found]
                if unfound:
                    logger.warning(f"Unfound: {unfound}")

    def convert_wse_to_pct(self):
        outpath_list = []
        for key, path in self.return_paths.items():
            return_pct = RETURNS.get(key)
            return_str = RETURN_NAME_LOOKUP.get(key)
            outpath = os.path.join(self.output_path, f"pct_ann_chance{return_str}.tif")
            if not os.path.exists(outpath):

                with rasterio.open(path) as src:
                    data = src.read(1)
                    valid_window = rasterio.windows.get_data_window(data)
                    valid_transform = rasterio.windows.transform(valid_window, src.transform)
                    data = data[valid_window.row_off:valid_window.row_off + valid_window.height,
                                      valid_window.col_off:valid_window.col_off + valid_window.width]

                    # Save profile for later
                    profile = src.profile
                    profile.update({
                        'height': valid_window.height,
                        'width': valid_window.width,
                        'transform': valid_transform
                    })

                    # Convert to dask array
                    dask_data = da.from_array(data, chunks=(2048, 2048), name=f"WSE{return_str}").astype(np.float32)

                    # Calculate the percent annual chance
                    pct_array = da.where(dask_data > 0, return_pct, -9999)

                    # Export the raster
                    export_raster(pct_array, outpath, profile=profile, transform=valid_transform, crs=src.crs, nodata=-9999)
                    logger.info(f"Exported: {outpath}")
                    logger.info(f"Profile: {profile}")
                    logger.info(f"CRS: {src.crs}")
            outpath_list.append(outpath)
        return outpath_list, self.vspecs


def convert_wse_to_pct_ann_chance(input_folder, output_folder):
    # Timer
    t1_start = process_time()
    tb_times = []

    conversions = WSEtoPercentAnnualChanceReturn(input_folder, output_folder)
    output_paths, specs = conversions.convert_wse_to_pct()
    tb_times.append(process_time())
    mosaic, profile = mosaic_rasters(output_paths, reduction='max')
    tb_times.append(process_time())

    next_lowest = get_next_pct_(mosaic, higher=False)
    next_highest = get_next_pct_(mosaic, higher=True)
    tb_times.append(process_time())

    print(f"Exporting Mosaic and Next Lowest and Next Highest Percent Grids...")
    # Export the mosaic
    outpath = os.path.join(output_folder, "pct_ann_chance_mosaic.tif")
    export_raster(mosaic, outpath, profile=profile, crs=specs.crs, transform=profile['transform'], nodata=-9999)
    tb_times.append(process_time())

    # Export the next lowest
    outpath_low = os.path.join(output_folder, "pct_ann_chance_mosaic_next_lowest.tif")
    export_raster(next_lowest, outpath_low, profile=profile, crs=specs.crs, transform=profile['transform'], nodata=-9999)
    tb_times.append(process_time())

    # Export the next highest
    outpath_high = os.path.join(output_folder, "pct_ann_chance_mosaic_next_highest.tif")
    export_raster(next_highest, outpath_high, profile=profile, crs=specs.crs, transform=profile['transform'], nodata=-9999)
    tb_times.append(process_time())

    return t1_start, tb_times


if __name__ == "__main__":

    input_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Perc_Chance_IN"
    output_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Return_PAC_Grids"

    sttime, times = convert_wse_to_pct_ann_chance(input_path, output_path)
    logger.info(f"Finished converting WSE to percent annual chance")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Input path: {input_path}")

    print(f" Processed percent chance in {times[-1]} - {times[0]} seconds")
    print(f" Time breakdown: ")
    for i, elapsed in enumerate(times):
        print(f"  {i}: {elapsed}")



