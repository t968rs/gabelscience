import geopandas as gpd
import typing as T
import os
import concurrent.futures
import rioxarray as rioxr
import xarray as xr


class Shrinkwrapraster:
    def __init__(self, input_polygons: gpd.GeoDataFrame,
                 raw_raster: T.Union[os.PathLike, str],
                 output_folder: T.Union[os.PathLike, str],
                 return_period: T.Union[float, str]) -> T.Union[os.PathLike, str]:
        """
        Initializes the Shrinkwrapraster object with the provided parameters.

        Parameters:
            input_polygons (gpd.GeoDataFrame): GeoDataFrame containing the polygons to use for the shrinkwrap.
            raw_raster (xr.DataArray): DataArray containing the raw raster data to be shrinkwrapped.
            output_folder (str): Path to the output folder for the shrinkwrapped raster.
        """

        self.input_polygons = input_polygons
        self.raw_raster = raw_raster
        self.output_folder = output_folder
        self.return_period = return_period

        self.output_raster = None