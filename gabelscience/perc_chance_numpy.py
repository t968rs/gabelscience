import os
import numpy as np
import pyresample as pyr
import dask.config
import dask.array as da
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from rasterio.warp import calculate_default_transform, reproject
from rasterio.mask import mask
from src.d00_utils.bounds_convert import bbox_to_gdf
import logging
from src.specs.raster_specs import create_raster_specs_from_path, create_raster_specs_from_xarray
from src.d00_utils.files_finder import get_raster_list
from src.d01_processing.export_raster import *
from src.d03_show.plot_raster import plot_raster
from rasterio.plot import show, show_hist


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


def gen_area_def(vspecs):
    return pyr.area_config.create_area_def(area_id="valid_area", projection=vspecs.crs,
                                           description="Valid Area for Percent Chance Analysis",
                                           area_extent=vspecs.bounds, resolution=vspecs.cellsizes,
                                           nprocs=10, verbose=True)


# then call this function:
logger = setup_logger()

RETURNS = {10: 0.002, 9: 0.010, 8: 0.020,
           7: 0.040, 6: 0.100}

RETURN_LOOKUP = {v: k for k, v in RETURNS.items()}

RETURN_NAMES = {"_0_2pct": 10, "_01pct": 9, "_02pct": 8,
                "_04pct": 7, "_10pct": 6}

RETURN_NAME_LOOKUP = {v: k for k, v in RETURN_NAMES.items()}

OUTPUT_SUBFOLDERS = ["00_DEM_Masked", "01_Elev_Grids", "02_WSE_Nearest_Ground", "03_PAC", "__INTMD__"]


class PercentAnnualChanceNumpy:

    def __init__(self, input_path, output_path, dem):
        self.input_path = input_path
        self.output_path = output_path
        self.input_dem_path = dem

        self.return_paths = {}
        self.xarrays_ready = {}
        self.valid_gdf = None
        self.dem = np.array([])
        self.empty_valid = None
        self.analysis_ds = np.array([])
        self.high_low_ds = np.array([])
        self.valid_loc = None

        # Find the WSE rasters
        self._find_wse_rasters()
        self._value_raster_specs = create_raster_specs_from_path(self.return_paths[10])
        self.valid_profile = {}
        self.valid_area_def = None
        self._all_raster_specs = {}
        self._set_configs()

        # Create area def
        self.valid_area_def = gen_area_def(self.vspecs)
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
        self._all_raster_specs["DEM"] = create_raster_specs_from_path(self.input_dem_path)

    @property
    def output_subfolders(self):

        return self._subfolder_paths()

    def _subfolder_paths(self):
        return [os.path.join(self.output_path, f) for f in OUTPUT_SUBFOLDERS]

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

    def open_valid_raster(self):
        """
        Open the valid raster and DEM using NumPy and rasterio
        """
        with rasterio.open(self.return_paths[10]) as src:
            valid_array = src.read(1)
            profile = src.profile

        # Round it to 2 decimal places
        valid_array = da.from_array(valid_array, chunks=(2048, 2048))
        valid_array = valid_array.round(2)

        # Export the valid raster
        profile.update(nodata=-9999)
        valid_array[valid_array == src.nodata] = -9999
        export_raster(valid_array, os.path.join(self.output_subfolders[1], "WSE_0_2pct.tif"), profile=profile)

        # Store TF valid locations
        self.valid_loc = valid_array != -9999
        # test_valid = np.where(self.valid_loc, 1, 99)
        # test_export_array(test_valid, self.output_subfolders[4], 123)

        # Calculate bounds
        bounds = src.bounds
        self.valid_gdf = bbox_to_gdf(bounds, self.vspecs.crs, "valid_bounds", self.output_path)
        self.valid_profile = profile

        # Creat empty valid array
        self.empty_valid = np.ones_like(valid_array, dtype=np.float32)
        # self.empty_valid = da.from_array(self.empty_valid, chunks=(2048, 2048))
        self.empty_valid = da.where(self.empty_valid == 1, -9999, np.nan)
        print(f"Empty Valid: {self.empty_valid}")

        return valid_array

    def open_dem_raster(self, varray):
        """
        Open the DEM using NumPy and rasterio
        """
        with rasterio.open(self.input_dem_path) as src:
            # Crop DEM to valid raster
            dem_array, transform = mask(src, self.valid_gdf.geometry, crop=True, pad=True, all_touched=True)
            # show(dem_array, transform=transform)
            profile = src.profile
            dem_array = da.from_array(dem_array[0], chunks=(2048, 2048))
            print(f"DEM Array: {dem_array}")

            # Assign attributes
            profile.update({
                "crs": src.crs,
                "BandName": "DEM",
                "long_name": "Digital Elevation Model",
                "DataType": "Elevation",
                "nodata": -9999
            })

        # Mask the DEM where the valid raster == True
        dem_array = da.where(self.valid_loc, dem_array, -9999)
        dem_array = dem_array.round(2)
        dem_to_plot = dem_array.compute()
        max_val = int(np.max(dem_array))
        plot_raster(dem_to_plot, title="DEM, Masked", vmin=0, vmax=max_val)
        show_hist(dem_to_plot, lw=0.0)

        # Align the DEM and valid raster
        input_area_def = pyr.area_config.create_area_def(area_id="DEM_area", projection=src.crs, description="DEM Area",
                                                         area_extent=src.bounds, resolution=(src.res[0], src.res[1]),
                                                         nprocs=10, verbose=True)
        print(f"Input Area Def: {input_area_def}")
        nn_image = pyr.image.ImageContainerNearest(dem_to_plot, input_area_def, radius_of_influence=10)
        dem_rs = nn_image.resample(self.valid_area_def)
        dem_array = dem_rs.image_data
        dem_array = da.from_array(dem_array, chunks=(2048, 2048))
        # dem_pix_info = pyr.kd_tree.get_neighbour_info(input_area_def, self.valid_area_def, radius_of_influence=10,
        #                                               neighbours=8, nprocs=10)
        # dem_array = pyr.kd_tree.get_sample_from_neighbour_info(data=dem_array, neighbour_info=dem_pix_info,)
        print(f"\nDEM Array: {dem_array}")
        # dem_array = reproject(
        #     source=dem_array,
        #     destination=np.empty_like(varray),
        #     src_transform=transform,
        #     src_crs=self.all_specs["DEM"].crs,
        #     dst_transform=self.vspecs.transform,
        #     dst_crs=self.vspecs.crs,
        #     resampling=Resampling.bilinear
        # )[0]

        # Update the stats attributes
        stats = {
            "STATISTICS_STDDEV": np.round(np.std(dem_array), 2),
            "STATISTICS_MEAN": np.round(np.mean(dem_array), 2),
            "STATISTICS_MAXIMUM": np.round(np.max(dem_array), 2),
            "STATISTICS_COUNT": np.round(np.count_nonzero(dem_array != -9999), 2),
            "STATISTICS_MINIMUM": np.round(np.min(dem_array), 2),
        }
        profile.update(stats)

        # Export the masked DEM
        export_raster(dem_array, os.path.join(self.output_subfolders[0], "DEM_masked.tif"), profile=profile)

        # Convert to dataset
        self.analysis_ds = {"Ground": dem_array, "WSE_0_2pct": varray}

        return dem_array

    def open_wse_rasters(self):
        """
        Open the WSE rasters using NumPy and rasterio
        """

        for key, path in self.return_paths.items():
            with rasterio.open(path) as src:
                profile = self.valid_profile.copy()
                profile.update(transform=src.transform)

                # Clip to relevant extent
                wse_array, transform = mask(src, self.valid_gdf.geometry, crop=True, pad=True, all_touched=True)
                wse_array = np.where(self.valid_loc, wse_array, self.empty_valid)

                # Store band 1
                wse_array = wse_array[0]

            # Rename the variable
            new_varname = f"WSE{RETURN_NAME_LOOKUP.get(key, 'WSE_UNK')}"

            # Reindex to the DEM
            wse_array = reproject(
                source=wse_array,
                destination=np.empty_like(self.analysis_ds["Ground"], dtype=np.float32),
                src_transform=transform,
                src_crs=src.crs,
                dst_transform=self.vspecs.transform,
                dst_crs=src.crs,
                resampling=Resampling.nearest
            )[0]

            # Round the WSE to 2 decimal places
            wse_array = np.round(wse_array, 2)
            wse_array = np.where(wse_array != 0, wse_array, self.empty_valid)

            # Update the stats attributes
            stats = {
                "STATISTICS_STDDEV": np.round(np.std(wse_array), 2),
                "STATISTICS_MEAN": np.round(np.mean(wse_array), 2),
                "STATISTICS_MAXIMUM": np.round(np.max(wse_array), 2),
                "STATISTICS_COUNT": np.round(np.count_nonzero(wse_array != -9999), 2),
                "STATISTICS_MINIMUM": np.round(np.min(wse_array), 2),
            }
            profile.update(stats)

            # Export the WSE raster
            outpath = os.path.join(self.output_subfolders[1], f"{new_varname}.tif")
            export_raster(wse_array, outpath, profile=profile)

            # Add to analysis dataset
            self.analysis_ds[new_varname] = wse_array

        return self.analysis_ds

    def create_pct_start_grid(self):
        """
        Create percent chance start grid using NumPy and rasterio
        """
        for varname, array in self.analysis_ds.items():
            if "WSE" not in varname:
                continue

            # Get % chance from varname
            perc_chance_name = varname.split("WSE")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                continue

            # Create percent chance array
            pct_chance_array = np.where(array > 0, perc_chance, np.nan)

            # Add the pct chance array to the dataset
            self.analysis_ds[f"pct_{varname}"] = pct_chance_array

        # Create high_low_ds
        self.high_low_ds = {k: v for k, v in self.analysis_ds.items() if "pct_" in k}
        self.analysis_ds = {k: v for k, v in self.analysis_ds.items() if "pct_" not in k}

        # Get max percent chance
        stacked_chance = np.stack(list(self.high_low_ds.values()), axis=-1)
        max_pct = np.nanmax(stacked_chance, axis=-1)
        self.high_low_ds["pct_start"] = max_pct

        # Export the pct start
        profile = self._get_profile()
        export_raster(self.high_low_ds["pct_start"], profile, os.path.join(self.output_subfolders[2], "pct_start.tif"))

        return self.high_low_ds

    def compute_next_high_next_low(self):
        """
        Compute next highest and lowest percent chance using NumPy and rasterio
        """
        # Initialize next highest and lowest percent chance arrays
        next_high = np.full_like(self.valid_loc, np.nan, dtype=np.float32)
        next_low = np.full_like(self.valid_loc, np.nan, dtype=np.float32)

        for varname, array in self.high_low_ds.items():
            if "pct_" not in varname:
                continue

            perc_chance_name = varname.split("pct_")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                continue

            # Find matching locations
            tolerance = 1e-2
            high_diff = np.abs(self.high_low_ds["pct_start"] - perc_chance)
            low_diff = np.abs(self.high_low_ds["pct_start"] - perc_chance)
            pct_high_match = np.where(high_diff < tolerance, 1, 0)
            pct_low_match = np.where(low_diff < tolerance, 1, 0)

            # Update next highest and lowest percent chance arrays
            next_high = np.where(pct_high_match, perc_chance, next_high)
            next_low = np.where(pct_low_match, perc_chance, next_low)

        # Fill missing values
        lowest_return_pct = RETURN_LOOKUP.get(min(RETURNS.keys()), None)
        next_low = np.where(np.isnan(next_low) & self.valid_loc, lowest_return_pct, next_low)

        highest_return_pct = RETURN_LOOKUP.get(max(RETURNS.keys()), None)
        next_high = np.where(np.isnan(next_high) & self.valid_loc, highest_return_pct, next_high)

        # Add to high_low_ds
        self.high_low_ds["NextHighestPct"] = next_high
        self.high_low_ds["NextLowestPct"] = next_low

        # Export the next highest and lowest percent chance
        profile = self._get_profile()
        export_raster(self.high_low_ds["NextHighestPct"], profile,
                      os.path.join(self.output_subfolders[2], "NextHighestPct.tif"))
        export_raster(self.high_low_ds["NextLowestPct"], profile,
                      os.path.join(self.output_subfolders[2], "NextLowestPct.tif"))

        return self.high_low_ds

    def process_perc_chance(self):
        """
        Process the percent chance data
        """
        # Open the valid raster
        valid_array = self.open_valid_raster()
        print(valid_array)

        # Open the DEM raster
        dem_array = self.open_dem_raster(valid_array)

        # Open the WSE rasters
        # self.open_wse_rasters()

        # Create the percent chance start grid
        # self.create_pct_start_grid()

        # Compute the next highest and lowest percent chance
        # self.compute_next_high_next_low()

        return self.analysis_ds, self.high_low_ds


if __name__ == "__main__":
    input_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance"
    output_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance_OUT"
    dem_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\test_perc_chance\ground_dem_CY.tif"

    pcg = PercentAnnualChanceNumpy(input_folder, output_folder, dem_path)
    pcg.process_perc_chance()
