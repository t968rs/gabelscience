from tqdm import tqdm
import dask.config
import rasterio
from src.d01_processing.export_raster import *
from src.d00_utils.specs import create_raster_specs_from_path
from src.d00_utils.files_finder import get_raster_list
import rasterio.windows
from rasterio.enums import Resampling
from rasterio.vrt import WarpedVRT
import logging
from src.d00_utils.timer import timer_wrap


# Set up the logger
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


def load_raster_as_dask_array(raster_path, fill_value: T.Union[float, int] = -9999):
    with rasterio.open(raster_path) as src:
        data = src.read(1)
        profile = src.profile
        filename = os.path.basename(raster_path)
        name = os.path.splitext(filename)[0]
        dask_data = da.from_array(data, chunks=(2048, 2048), name=name).astype(np.float32)
        dask_data = da.where(dask_data == fill_value, np.nan, dask_data)
    return dask_data, profile


def load_VRT_raster_as_dask_array(raster_path, vrt_opts, fill_value: T.Union[float, int] = -9999):
    with rasterio.open(raster_path) as src:
        with WarpedVRT(src, **vrt_opts) as vrt:
            # At this point 'vrt' is a full dataset with dimensions,
            # CRS, and spatial extent matching 'vrt_options'.

            # Read all data into memory.
            data = vrt.read(1)
            profile = vrt.profile
            filename = os.path.basename(raster_path)
            name = os.path.splitext(filename)[0]
            dask_data = da.from_array(data, chunks=(2048, 2048), name=name).astype(np.float32)
            dask_data = da.where(dask_data == fill_value, np.nan, dask_data)
            print(f'VRT Dask: {dask_data}')
            print(f'VRT Profile: {profile}')
            print('Max: ', da.nanmax(dask_data).compute())

    return dask_data, profile


class GenerateNextHighestLowest:
    def __init__(self, dem_path, wse_folder, out_folder):
        self.ground_path = dem_path
        self.out_folder = out_folder
        self.input_path = wse_folder
        self.filled_wse_paths = []
        self.return_paths = {}
        self._all_raster_specs = {}
        self._set_configs()

        # Find the WSE rasters
        self._find_wse_rasters()
        self._value_raster_specs = create_raster_specs_from_path(self.return_paths[10])

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

    def find_next_highest_lowest_wse(self, wse_lookup, dem_profile, dem_array):

        # Initialize arrays for next highest and next lowest WSE
        next_highest = da.full_like(dem_array, np.nan, dtype=np.float32)
        next_lowest = da.full_like(dem_array, np.nan, dtype=np.float32)

        # Iterate through each WSE raster # filled_wse_rasters[wse_path] = (filled_wse_data, wse_profile)
        print(f"DEM Shape: {dem_array.shape}")
        pbar = tqdm(wse_lookup.items(), desc="Finding next highest and lowest WSE")
        pbar_dict = {}
        for wse_path, wse_data in wse_lookup.items():  # filled_wse_rasters[wse_path] = filled_wse_data
            pbar_dict["WSE"] = os.path.basename(wse_path)
            pbar_dict["Shape"] = wse_data.shape
            pbar.set_postfix(pbar_dict)

            # Update next highest WSE
            higher_mask = (wse_data > dem_array) & (da.isnan(next_highest) | (wse_data < next_highest))
            next_highest[higher_mask] = wse_data[higher_mask]

            # Update next lowest WSE
            lower_mask = (wse_data < dem_array) & (da.isnan(next_lowest) | (wse_data > next_lowest))
            next_lowest[lower_mask] = wse_data[lower_mask]

            pbar.update(1)
        return next_highest, next_lowest, dem_profile

    def fill_wse_rasters(self):
        # Load the DEM raster as a Dask array
        print(f"Loading DEM raster: {self.ground_path}")
        dem_data, dem_profile = load_raster_as_dask_array(self.ground_path)
        dem_window = rasterio.windows.get_data_window(dem_data)
        dem_transform = rasterio.windows.transform(dem_window, dem_profile.get("transform"))
        dem_data = dem_data[dem_window.row_off:dem_window.row_off + dem_window.height,
               dem_window.col_off:dem_window.col_off + dem_window.width]
        dem_profile.update(transform=dem_transform, height=dem_window.height, width=dem_window.width)

        # Load the largest WSE raster as a Dask array
        print(f"Loading WSE raster: {self.return_paths[10]}")
        wse_data, wse_profile = load_raster_as_dask_array(self.return_paths[10])
        valid_window = rasterio.windows.get_data_window(wse_data)
        valid_window = rasterio.windows.intersection(valid_window, dem_window)
        wse_data = wse_data[valid_window.row_off:valid_window.row_off + valid_window.height,
                     valid_window.col_off:valid_window.col_off + valid_window.width]
        valid_loc = wse_data != -9999

        # Determine valid DEM window and mask DEM by valid wse valid data
        print(f"Masking DEM data")
        dem_data = dem_data[valid_window.row_off:valid_window.row_off + valid_window.height,
                   valid_window.col_off:valid_window.col_off + valid_window.width]
        dem_transform = rasterio.windows.transform(valid_window, dem_profile.get("transform"))
        dem_data = da.where(valid_loc, dem_data, -9999)
        dem_profile.update(transform=dem_transform, height=valid_window.height, width=valid_window.width)

        # Export the DEM raster
        dem_profile.update(nodata=-9999)
        outpath = os.path.join(self.out_folder, "ground_dem.tif")
        export_raster(dem_data, outpath, profile=dem_profile, nodata=-9999,
                      crs=self.vspecs.crs, transform=dem_transform)

        # Initialize a dictionary to store the filled WSE rasters
        filled_wse_rasters = {}

        # Iterate through each WSE raster path
        print(f"Filling WSE rasters")
        pbar = tqdm(total=2 * len(self.return_paths.items()), desc="Filling WSE rasters")
        for key, wse_path in self.return_paths.items():

            if key is None:
                raise ValueError(f"RETURN key not found in path: {wse_path}")

            wse_name = os.path.basename(wse_path)
            wse_name = os.path.splitext(wse_name)[0]
            outpath = os.path.join(self.out_folder, f"{wse_name}_filled.tif")

            if os.path.exists(outpath):
                logger.info(f"Skipping {wse_name} as it already exists")
                continue
            pbar.set_postfix({"WSE": wse_name})

            # Load the WSE raster as a Dask array
            #wse_data, wse_profile = load_raster_as_dask_array(wse_path, fill_value=-9999)
            vrt_options = {
                'resampling': Resampling.cubic,
                'crs': self.vspecs.crs,
                'transform': dem_transform,
                'height': valid_window.height,
                'width': valid_window.width,
            }

            wse_data, vrt_profile = load_VRT_raster_as_dask_array(wse_path, vrt_options, fill_value=-9999)

            # Calculate the fill value based on the DEM and RETURN key
            fill_value = dem_data - (10 - key) * 0.1

            # Empty bin
            empty_nest = da.full_like(dem_data, -9999, dtype=np.float32)

            wse_centered = da.where(wse_data != -9999, wse_data, empty_nest)

            # Fill the WSE raster where there is valid DEM data
            fill_where = valid_loc & (wse_data == -9999)
            filled_wse_data = da.where(fill_where, fill_value, wse_centered)

            # Store the filled WSE raster in the dictionary
            filled_wse_rasters[wse_path] = filled_wse_data
            pbar.update(1)

            # Export the filled WSE raster
            export_raster(filled_wse_data, outpath, profile=wse_profile, nodata=-9999,
                          crs=self.vspecs.crs, transform=dem_transform)

        return filled_wse_rasters, dem_profile, dem_data

def generate_next_highest_lowest_wse(dem_path, wse_folder, out_folder):
    hl_setup = GenerateNextHighestLowest(dem_path, wse_folder, out_folder)  # filled_wse_rasters[wse_path] = (filled_wse_data, wse_profile)
    filled_wse_lookup, dem_profile, dem_array = hl_setup.fill_wse_rasters()
    nh, nl, d_profile = hl_setup.find_next_highest_lowest_wse(filled_wse_lookup, dem_profile, dem_array)

    # Export the next highest and next lowest WSE rasters
    outpath = os.path.join(wse_folder, "next_highest.tif")
    export_raster(nh, outpath, profile=d_profile, nodata=-9999,
                  crs=hl_setup.vspecs.crs, transform=d_profile.get("transform"))
    outpath = os.path.join(wse_folder, "next_lowest.tif")
    export_raster(nl, outpath, profile=d_profile, nodata=-9999,
                  crs=hl_setup.vspecs.crs, transform=d_profile.get("transform"))



if __name__ == '__main__':
    ground_path = r"E:\Iowa_3B\02_mapping\Maple_Mapping\Terrain\DEM_vft_3417.tif"
    input_path = r"E:\Iowa_3B\03_delivery\Floodplain\Maple_10230005\Supplemental_Data\Rasters\WSE"
    output_folder = r"E:\Iowa_3B\02_mapping\Maple_Mapping\NH_NL_Grids"
    timer_wrap(generate_next_highest_lowest_wse)(ground_path, input_path, output_folder)