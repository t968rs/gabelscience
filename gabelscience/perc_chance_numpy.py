import pyresample as pyr
import dask.config
import rasterio
from rasterio.mask import mask
import rasterio.windows
from dask.diagnostics import ProgressBar

from src.d00_utils.gbounds import bbox_to_gdf
import logging
from src.d00_utils.specs import create_raster_specs_from_path
from src.d00_utils.files_finder import get_raster_list
from src.d01_processing.export_raster import *
from src.d03_show.plot_raster import plot_raster


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

def resample_block(dem_block, input_area_def, valid_area_def):
    resampler = pyr.kd_tree.NumpyResamplerBilinear(input_area_def, valid_area_def)
    return resampler.resample(dem_block).image_data

def gen_area_def(vspecs):
    return pyr.area_config.create_area_def(area_id="valid_area", projection=vspecs.crs,
                                           description="Valid Area for Percent Chance Analysis",
                                           area_extent=vspecs.bounds, resolution=vspecs.cellsizes,
                                           nprocs=10, verbose=True)


def get_higher_lower_pct_return(pct_grids, varname):
    min_return, max_return = min(RETURNS.keys()), max(RETURNS.keys())

    categorical = pct_grids[varname]
    bins = pct_grids['bins']

    # Iterate through the RETURNS dictionary
    for return_ord, return_pct in RETURNS.items():
        lower_return = RETURNS.get(return_ord - 1, RETURNS[min_return])
        higher_return = RETURNS.get(return_ord + 1, RETURNS[max_return])

        # Get equivalent locations using the bin index
        return_index = np.digitize([return_pct], bins, right=False)[0] - 1
        equiv = categorical == return_index

        # Update NextLowestPct grid
        pct_grids['NextLowestPct'] = da.where(equiv, lower_return, pct_grids['NextLowestPct'])

        # Update NextHighestPct grid
        pct_grids['NextHighestPct'] = da.where(equiv, higher_return, pct_grids['NextHighestPct'])

    return pct_grids


def calc_stats_dask(array, nodata=-9999):
    # Update the stats attributes
    if not isinstance(array, da.Array):
        print("Array is not a dask array")
        print(f" Array: {array}, \n\tType: {type(array)}")
    array_not_null = da.where(array != nodata, array, np.nan)

    mean, std, max_val, min_val, count = (da.nanmean(array_not_null).round(2),
                                          da.nanstd(array_not_null).round(2),
                                          da.nanmax(array_not_null).round(2),
                                          da.nanmin(array_not_null).round(2),
                                          da.count_nonzero(array_not_null).round(2))

    print(f" \tGetting Stats...")
    with ProgressBar():
        mean, std, max_val, min_val, count = dask.compute(mean, std, max_val, min_val, count)

    stats = {
        "STATISTICS_STDDEV": std,
        "STATISTICS_MEAN": mean,
        "STATISTICS_MAXIMUM": max_val,
        "STATISTICS_COUNT": count,
        "STATISTICS_MINIMUM": min_val,
    }

    for key, value in stats.items():
        if "STATISTICS" in key:
            key = key.split("STATISTICS_")[1]
        print(f"\t\t{key}: {value}")

    return stats


def auto_chunk_dask(array, chunk_size=2048):
    if not isinstance(array, da.Array):
        return
    da_shape = array.shape
    # print(f"\t\tDA Shape: {da_shape}")
    if len(da_shape) == 2:
        chunks = (chunk_size, chunk_size)
    elif len(da_shape) == 3:
        chunks = (chunk_size, chunk_size, 1)
    else:
        raise ValueError(f"Array shape not supported: {da_shape}")
    return array.rechunk(chunks)


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
        self.dem = None
        self.empty_valid = None
        self.analysis_ds = {}
        self.high_low_ds = {}
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

        with rasterio.open(self.input_dem_path) as src:
            dem_window = rasterio.windows.get_data_window(src.read(1), nodata=-9999)
        with rasterio.open(self.return_paths[10]) as src:
            valid_array = src.read(1)

            # Crop the valid raster to the DEM window
            valid_window = rasterio.windows.get_data_window(valid_array, nodata=-9999)
            valid_window = valid_window.intersection(dem_window)
            valid_transform = rasterio.windows.transform(valid_window, src.transform)
            valid_bounds = rasterio.windows.bounds(valid_window, valid_transform)
            valid_array = valid_array[valid_window.row_off:valid_window.row_off + valid_window.height,
                            valid_window.col_off:valid_window.col_off + valid_window.width]
            profile = src.profile
            profile.update(transform=valid_transform, nodata=-9999, bounds=valid_bounds, valid_window=valid_window)

            # Calculate bounds
            self.valid_gdf = bbox_to_gdf(valid_bounds, self.vspecs.crs, "valid_bounds", self.output_path)
            self.valid_profile = profile
            self.vspecs.update(bounds=valid_bounds, transform=valid_transform)

        # Round it to 2 decimal places
        valid_array = da.from_array(valid_array, chunks=(2048, 2048))
        valid_array = valid_array.round(2)
        outpath = os.path.join(self.output_subfolders[4], "valid_array.tif")
        zero_array = da.where(valid_array != -9999, 1, 99)
        with rasterio.open(outpath, 'w', **profile) as dst:
            dst.write(zero_array, 1)

        # Export the valid raster

        print(self.vspecs.__repr__())
        # print(f"CRS: {self.vspecs.crs}")
        export_raster(valid_array, os.path.join(self.output_subfolders[1], "WSE_0_2pct.tif"),
                      profile=profile, crs=self.vspecs.crs)

        # Store TF valid locations
        self.valid_loc = valid_array != -9999

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
            dem_array, transform = mask(src, self.valid_gdf.geometry, crop=True, pad=True, all_touched=False)
            dem_array = dem_array[self.valid_profile['valid_window'].row_off:self.valid_profile['valid_window'].row_off +
                                    self.valid_profile['valid_window'].height, self.valid_profile['valid_window'].col_off:
                                    self.valid_profile['valid_window'].col_off + self.valid_profile['valid_window'].width]
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
        empty = da.full_like(self.empty_valid, -9999, dtype=np.float32)
        dem_array = da.where(self.valid_loc, dem_array, empty)
        dem_array = dem_array.round(2)

        plot_raster(dem_array, title="DEM, Masked", value="Elevation")
        # plot_histogram(dem_array, title="DEM Histogram", xlabel="Elevation", ylabel="Frequency")

        print(f"\nTransform: \n{src.transform}")
        print(f"\nProfile: \n{profile}")
        src_transform = np.array(src.transform).reshape(3, 3)
        dst_transform = np.array(self.valid_profile['transform']).reshape(3, 3)

        # Check if the transformation matrices are identity matrices
        if np.allclose(src_transform, np.eye(3)) or np.allclose(dst_transform, np.eye(3)):
            raise ValueError(
                "One of the transformation matrices is an identity matrix, which is not valid for georeferencing.")
        if not np.array_equal(src_transform, dst_transform):
            print(f"\nTransforms: \n\tSRC: {src_transform}\n\tDST: {dst_transform}\n\tnot quite equal...")
        print(f"\nDEM Array: {dem_array}")

        # Calc stats and update profile
        stats = calc_stats_dask(dem_array)
        profile.update(stats)

        # Export the masked DEM
        export_raster(dem_array, os.path.join(self.output_subfolders[0], "DEM_masked.tif"),
                      profile=profile, crs=self.vspecs.crs)

        # Convert to dataset
        self.analysis_ds = {"Ground": dem_array, "WSE_0_2pct": varray}

        return dem_array

    def open_wse_rasters(self):
        """
        Open the WSE rasters using NumPy and rasterio
        """

        fill_counter = 0.1
        for key, path in self.return_paths.items():
            with rasterio.open(path) as src:
                profile = self.valid_profile.copy()
                # Crop WSE to valid raster
                wse_array, transform = mask(src, self.valid_gdf.geometry, crop=True, pad=True, all_touched=True)
                profile.update(transform=transform)

                profile = src.profile
                wse_array = da.from_array(wse_array[0], chunks=(2048, 2048))
                print(f"WSE Array: {wse_array}")

                # Assign attributes
                profile.update({
                    "crs": src.crs,
                    "BandName": "WSE",
                    "long_name": "Water Surface Elevation",
                    "DataType": "Elevation",
                    "nodata": -9999
                })

            # Rename the variable
            new_varname = f"WSE{RETURN_NAME_LOOKUP.get(key, 'WSE_UNK')}"

            # Round, mask, fill the WSE
            empty = da.full_like(self.empty_valid, -9999, dtype=np.float32)
            filler_array = da.where(self.valid_loc, (self.analysis_ds["Ground"] - fill_counter), -9999)
            fill_counter += 0.1
            wse_array = da.where(self.valid_loc, wse_array, empty)
            wse_array = da.where(wse_array == empty, filler_array, wse_array)
            wse_array = wse_array.round(2)

            # Calc stats and update profile
            # stats = calc_stats_dask(wse_array)
            # profile.update(stats)

            # Export the WSE raster
            # outpath = os.path.join(self.output_subfolders[1], f"{new_varname}.tif")
            # export_raster(wse_array, outpath,
            #           profile=profile, crs=self.vspecs.crs)

            # Add to analysis dataset
            self.analysis_ds[new_varname] = wse_array

        return self.analysis_ds

    def create_pct_start_grid(self):
        """
        Create percent chance start grid using NumPy and rasterio
        """
        ds_iterator = {k: v for k, v in self.analysis_ds.items() if "WSE" in k}
        for varname, array in ds_iterator.items():
            if "WSE" not in varname:
                continue

            # Get % chance from varname
            perc_chance_name = varname.split("WSE")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                continue

            # Create percent chance array
            empty = da.full_like(self.empty_valid, -9999, dtype=np.float32)
            pct_chance_array = da.where(array > 0, perc_chance, empty)

            # Add the pct chance array to the dataset
            self.analysis_ds[f"pct_{varname}"] = pct_chance_array

        # Create high_low_ds
        self.high_low_ds = {k: v for k, v in self.analysis_ds.items() if "pct_" in k}
        self.analysis_ds = {k: v for k, v in self.analysis_ds.items() if "pct_" not in k}

        # Get max percent chance
        stacked_chance = da.stack(list(self.high_low_ds.values()), axis=-1)
        max_pct = da.max(stacked_chance, axis=-1)
        self.high_low_ds["pct_start"] = max_pct

        # Convert to categorical
        sorted_returns = sorted(RETURNS.values())
        bins = sorted_returns + [1]
        cat_start = da.digitize(max_pct, bins, right=False) - 1
        self.high_low_ds["cat_start"] = cat_start
        self.high_low_ds["bins"] = bins
        print(f"Categorical Start: {self.high_low_ds['cat_start']}")
        print(f"Bins: {bins}")

        # Export the pct start
        profile = self.valid_profile
        export_raster(self.high_low_ds["pct_start"], os.path.join(self.output_subfolders[2], "pct_start.tif"),
                      profile=profile, crs=self.vspecs.crs, nodata=-9999)

        return self.high_low_ds

    def compute_next_high_next_low_pct(self):
        # Initialize NextLowestPct and NextHighestPct grids
        min_return, max_return = min(RETURNS.keys()), max(RETURNS.keys())
        self.high_low_ds['NextLowestPct'] = da.full_like(self.empty_valid, RETURNS[min_return], dtype=np.float32)
        self.high_low_ds['NextHighestPct'] = da.full_like(self.empty_valid, RETURNS[max_return], dtype=np.float32)

        # Mask the grids
        self.high_low_ds["NextLowestPct"] = da.where(self.valid_loc, self.high_low_ds["NextLowestPct"], -9999)
        self.high_low_ds["NextHighestPct"] = da.where(self.valid_loc, self.high_low_ds["NextHighestPct"], -9999)
        plot_raster(self.high_low_ds["NextLowestPct"], title="NextLowestPct", value="Pct Chance", vmin=0, vmax=1,
                    stops_list=list(RETURNS.values()))

        # Compute next highest and lowest perc-chance return periods for each location
        self.high_low_ds = get_higher_lower_pct_return(self.high_low_ds, 'cat_start')
        print(f"Ann. % Chance Variables: \n\t{self.high_low_ds.keys()}")

        # Convert to categorical
        sorted_returns = sorted(RETURNS.values())
        bins = sorted_returns + [np.inf]
        for key in ["NextLowestPct", "NextHighestPct"]:
            pct_array = self.high_low_ds[key]
            cat_array = da.digitize(pct_array, bins, right=False) - 1
            self.high_low_ds[f"cat_{key}"] = cat_array

        # Export the next highest and lowest
        self.high_low_ds = {k: auto_chunk_dask(v) for k, v in self.high_low_ds.items()}
        for varname in ["NextLowestPct", "NextHighestPct"]:
            array = auto_chunk_dask(self.high_low_ds[varname])
            export_raster(array, os.path.join(self.output_subfolders[2], f"{varname}.tif"),
                       profile=self.valid_profile, crs=self.vspecs.crs, nodata=-9999)
            self.high_low_ds[varname] = da.where(array != -9999, array, np.nan)

    def compute_next_high_next_low_elev(self):

        lowest_elevations = f"WSE{RETURN_NAME_LOOKUP.get(min(RETURNS.keys()))}"
        highest_elevations = f"WSE{RETURN_NAME_LOOKUP.get(max(RETURNS.keys()))}"

        # Init next highest and lowest elevation arrays
        self.high_low_ds["NextHighestElevation"] = da.full_like(self.empty_valid, fill_value=-9999, dtype=np.float32)
        self.high_low_ds["NextLowestElevation"] = da.full_like(self.empty_valid, fill_value=-9999, dtype=np.float32)

        # Mask the grids
        self.high_low_ds["NextLowestElevation"] = da.where(self.valid_loc, self.high_low_ds["NextLowestPct"], -9999)
        self.high_low_ds["NextHighestElevation"] = da.where(self.valid_loc, self.high_low_ds["NextHighestPct"], -9999)

        # Fill each with elevations from highest and lowest WSE
        self.high_low_ds["NextHighestElevation"] = da.where(self.analysis_ds[highest_elevations],
                                                            self.analysis_ds[highest_elevations],
                                                            self.high_low_ds["NextHighestElevation"])
        self.high_low_ds["NextLowestElevation"] = da.where(self.analysis_ds[lowest_elevations],
                                                           self.analysis_ds[lowest_elevations],
                                                           self.high_low_ds["NextLowestElevation"])

        print(f"{self.high_low_ds.keys()}")

        for i, varname in enumerate(self.analysis_ds.keys()):
            if "WSE" not in varname:
                continue
            print(f"\nProcessing {varname} to get next highest and lowest elevations")

            # Get % chance from varname
            perc_chance_name = varname.split("WSE")[1]
            perc_chance = RETURNS.get(RETURN_NAMES.get(perc_chance_name, None), None)
            if not perc_chance:
                continue

            # Find matching locations
            tolerance = 0.001  # Adjust the tolerance as needed
            pct_high_match = da.abs(self.high_low_ds["NextHighestPct"] - perc_chance) < tolerance
            pct_low_match = da.abs(self.high_low_ds["NextLowestPct"] - perc_chance) < tolerance

            # num_high_cells = pct_high_match.sum().compute()
            # num_low_cells = pct_low_match.sum().compute()
            # print(f"\tAnn {perc_chance * 100}% Chance: {perc_chance} cells, H: {num_high_cells:}, L: {num_low_cells:}")

            # Get the elevations
            self.high_low_ds["NextHighestElevation"] = da.where(pct_high_match,
                                                                self.analysis_ds[varname],
                                                                self.high_low_ds["NextHighestElevation"])
            self.high_low_ds["NextLowestElevation"] = da.where(pct_low_match,
                                                                   self.analysis_ds[varname],
                                                                   self.high_low_ds["NextLowestElevation"])

            # outpath = os.path.join(self.output_subfolders[4], f"NextHighestElevations_{perc_chance}_{i}.tif")
            # export_raster(self.high_low_ds["NextHighestElevation"], outpath, profile=self.valid_profile, crs=self.vspecs.crs)
            # outpath = os.path.join(self.output_subfolders[4], f"NextLowestElevations_{perc_chance}_{i}.tif")
            # export_raster(self.high_low_ds["NextLowestElevation"], outpath, profile=self.valid_profile, crs=self.vspecs.crs)


        # Chunks them
        self.high_low_ds["NextHighestElevation"] = auto_chunk_dask(self.high_low_ds["NextHighestElevation"])
        self.high_low_ds["NextLowestElevation"] = auto_chunk_dask(self.high_low_ds["NextLowestElevation"])

        # Do some filling and masking
        self.high_low_ds["NextHighestElevation"] = da.where(self.high_low_ds["NextHighestElevation"] < self.analysis_ds["Ground"],
                                                            self.analysis_ds[highest_elevations],
                                                            self.high_low_ds["NextHighestElevation"])
        self.high_low_ds["NextLowestElevation"] = da.where(self.high_low_ds["NextLowestElevation"] > self.analysis_ds["Ground"],
                                                              self.analysis_ds[lowest_elevations],
                                                              self.high_low_ds["NextLowestElevation"])

        # Export the next highest and lowest elevations
        oupath = os.path.join(self.output_subfolders[2], "NextHighestElevations.tif")
        export_raster(self.high_low_ds["NextHighestElevation"], oupath, profile=self.valid_profile, crs=self.vspecs.crs)

        oupath = os.path.join(self.output_subfolders[2], "NextLowestElevations.tif")
        export_raster(self.high_low_ds["NextLowestElevation"], oupath, profile=self.valid_profile, crs=self.vspecs.crs)

    def compute_perc_annual_chance(self):
        """
        Compute the percent chance
        """

        # Set calc tolerance
        tolerance = 1e-4

        # Drop the WSE rasters
        to_drop = [v for v in self.analysis_ds.keys() if "WSE" in v]
        print(f"Variables to drop: {to_drop}\nStarting Percent-Annual-Chance Calculation...")
        for key in to_drop:
            self.analysis_ds.pop(key)
        ground_dem = self.analysis_ds["Ground"]
        del self.analysis_ds

        # Set up the variables
        y1 = self.high_low_ds["NextLowestElevation"]
        y2 = ground_dem
        y3 = self.high_low_ds["NextHighestElevation"]
        x1 = self.high_low_ds["NextLowestPct"]
        x3 = self.high_low_ds["NextHighestPct"]

        # Calculate demoninator difference
        denom = y3 - y1
        denom_0 = denom == 0
        denom = da.where(denom_0, -9999, denom)
        denom = da.where(self.valid_loc, denom, -9999)
        # test_export_array(denom, self.output_subfolders[4], 532, -9999)

        # Create and clip the numerator
        numerator = (y2 - y1) * (da.log10(x3) - da.log10(x1))
        max_num = da.max(numerator).compute()
        print(f"Numerator: {max_num}, {da.min(numerator).compute()}")
        # numerator = da.clip(numerator, None, max_num)

        # Compute the percent annual chance
        x2 = 10 ** ((y2 - y1) * (da.log10(x3) - da.log10(x1)) / denom + da.log10(x1))
        x2 = x2.astype(np.float32)
        x2 = auto_chunk_dask(x2)
        # plot_raster(x2, title="PAC", value="PAC", vmin=0, vmax=1)

        # Export the PAC
        print(f"Exporting PAC")
        pac_path = os.path.join(self.output_subfolders[3], "PAC.tif")

        x2 = da.where(x2 > 0.1, 0.1, x2)
        x2 = da.where(self.valid_loc, x2, -9999)
        export_raster(x2, pac_path, profile=self.valid_profile, crs=self.vspecs.crs)

        return x2

    def process_perc_chance(self):
        """
        Process the percent chance data
        """
        # Open the valid raster
        for f in self.output_subfolders:
            if not os.path.exists(f):
                os.makedirs(f)
        valid_array = self.open_valid_raster()
        print(valid_array)

        # Open the DEM raster
        dem_array = self.open_dem_raster(valid_array)

        # Open the WSE rasters
        self.open_wse_rasters()

        # Create the percent chance start grid
        self.create_pct_start_grid()

        # Compute the next highest and lowest percent chance
        self.compute_next_high_next_low_pct()

        # Compute the next highest and lowest elevations
        self.compute_next_high_next_low_elev()

        # Compute the percent annual chance
        self.compute_perc_annual_chance()



if __name__ == "__main__":
    input_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Perc_Chance_IN"
    output_folder = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\Perc_Chance_OUT"
    dem_path = r"E:\Iowa_1A\02_mapping\CoonYellow\Grids_CY\DEM\ground_dem_CY.tif"

    pcg = PercentAnnualChanceNumpy(input_folder, output_folder, dem_path)
    pcg.process_perc_chance()
