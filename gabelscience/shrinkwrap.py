import geopandas as gpd
import pandas as pd
import typing as T
from src.specs import literals
from src.specs.raster_specs import create_raster_specs_from_path
from src.d00_utils.system import file_size, get_system_memory
from src.d00_utils.regular_grids import create_regular_grid
from src.d03_show import cplotting, printers
import os
import shapely
import concurrent.futures
import dask
from dask.diagnostics import ProgressBar
import dask_geopandas
import rioxarray as rioxr
import xarray as xr


class ShrinkwrapRaster:
    def __init__(self, inputpolygons: str,
                 raw_raster: T.Union[os.PathLike, str],
                 outputfolder: T.Union[os.PathLike, str],
                 return_period: T.Union[float, str],
                 epsg_code: int, hole_size: int = 30000):
        """
        Initializes the Shrinkwrapraster object with the provided parameters.

        Parameters:
            inputpolygons (gpd.GeoDataFrame): GeoDataFrame containing the polygons to use for the shrinkwrap.
            raw_raster (xr.DataArray): DataArray containing the raw raster data to be shrinkwrapped.
            outputfolder (str): Path to the output folder for the shrinkwrapped raster.
        """

        self.input_polygons = inputpolygons
        self.raw_raster = raw_raster
        self.outputfolder = output_folder
        self.return_period = return_period
        self.epsg_code = epsg_code
        self.crs = "EPSG:" + str(epsg_code)
        self.hole_size = hole_size
        self._init_return_period()
        self.output_raster = None
        self.processing_gdf = None

        self.system_memory = get_system_memory()
        if self.system_memory > 64:
            self.raster_intmd = "memory"
        self.raster_specs = self._init_raster_specs()

    def _init_return_period(self):
        """
        Initializes the return period lookup for the specified return period.
        """
        from references.dicts import create_return_period
        self.return_period = create_return_period(self.return_period)

    def _create_output_folders(self):
        """
        Creates the output folders for the shrinkwrapped raster.
        """
        if os.path.split(self.outputfolder)[1] != "01_Shrinkwrap":
            self.output_folder = os.path.join(self.outputfolder, "01_Shrinkwrap")
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        # create output for this particular shrinkwrap
        outname = f"shrinkwrap_{self.return_period.return_file_string}"
        self.output_folder = os.path.join(self.output_folder, outname)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        # create subfolders
        subfolders = ['raster_intmd', 'vectors']
        for folder in subfolders:
            path = os.path.join(self.output_folder, folder)
            if not os.path.exists(path):
                os.makedirs(path)
        self.raster_intmd = os.path.join(self.output_folder, 'raster_intmd')
        self.vector_folder = os.path.join(self.output_folder, 'vectors')

    def _init_raster_specs(self):
        """
        Initializes the raster specifications for the input raster dataset.
        """
        specs = create_raster_specs_from_path(self.raw_raster)
        printers.print_attributes(specs, 2)
        return create_raster_specs_from_path(self.raw_raster)

    @staticmethod
    def define_howmany_splits(size_mb):

        if size_mb < 350:
            splits_count = 1
        elif size_mb < 1000:
            splits_count = 4
        elif size_mb < 3000:
            splits_count = 8
        else:
            splits_count = 16

        return splits_count

    @staticmethod
    def clip_it(ds: xr.Dataset,
                gdf: gpd.GeoDataFrame,
                crs: T.Union[str], alltouched: bool) -> xr.Dataset:
        """
        Clips the raster dataset using the provided GeoDataFrame and CRS.

        Parameters:
            ds (xarray.Dataset): The raster dataset to clip.
            gdf (geopandas.GeoDataFrame): The GeoDataFrame containing the clipping polygons.
            crs (str): The coordinate reference system to use for the clipping.
            alltouched (bool): Flag to include all touching pixels in the clip operation.

        Returns:
            xarray.Dataset: The clipped raster dataset.
        """

        ds.rio.write_crs(crs)

        rasterized = ds.rio.clip(gdf.geometry.values, crs=crs, all_touched=alltouched, from_disk=True, drop=False)

        return rasterized

    @dask.delayed
    def buffer_delayed(self, gdf):
        buffer = round(self.raster_specs.res, 1)
        gdf['geometry'] = gdf.geometry.buffer(buffer, resolution=4,
                                              cap_style="flat", join_style="mitre",
                                              mitre_limit=4)
        return gdf

    def remove_holes(self, gdf):
        interior_lines = gdf.geometry.interiors
        hole_pg, cuts, dangles, invalid = interior_lines.polygonize(full=True)
        small_holes = gpd.GeoDataFrame(geometry=hole_pg[hole_pg.area < self.hole_size])
        merged = (dask_geopandas
                  .from_geopandas(pd.concat([gdf, small_holes], ignore_index=True))
                  .spatial_shuffle(by="hilbert", npartitions=1000))
        diss = merged.dissolve()
        return diss

    def import_and_buffer_polygons(self):
        # Read Polygon Shapefile
        gdf = gpd.read_file(self.input_polygons)
        gdf = gdf.to_crs(self.crs).loc[:, ["geometry"]]
        print(f' Converting polygons to dask...')
        gddf = dask_geopandas.from_geopandas(gdf).spatial_shuffle(by="hilbert", npartitions=1000)

        # Perform buffer
        print(f' Buffering polygons...')
        delayed_buff = self.buffer_delayed(gddf)
        print(f"Read polygon shapefile\n {gddf.columns}")
        with ProgressBar():

            gddf = delayed_buff.compute()

        # Split it up for processing
        grid_gdf, n_cells, grid_features = create_regular_grid(gddf, n_cells=(10, 10),
                                                               epsg_code=self.epsg_code,
                                                               overlap=False)
        grid_gdf.to_file(os.path.join(self.vector_folder, "grid.shp"), engine="fiona", index=True)
        intersected = grid_gdf.overlay(gddf, how='intersection', keep_geom_type=True)
        intersected = intersected.explode(index_parts=False).loc[:, ["geometry"]]
        intersected["values"] = 1
        print(f' Prepped polygons\n {intersected.columns}')
        cplotting.plot_map_data_to_html(intersected, self.output_folder, "intersect")
        self.processing_gdf = intersected

    def open_raster_as_ones(self):
        # Open the raster as dataset
        ds = rioxr.open_rasterio(self.raw_raster, chunks={"x": 2048, "y": 2048},
                                 lock=False, band_as_variable=True)
        print(f"\n----- Input Raster:\n{ds}")
        ds = ds.rename_vars({"band_1": "food_raster"})
        variable0 = [v for v in ds.data_vars][0]
        nodata_value = ds[variable0].rio.nodata
        print(f' {variable0} NODATA: {nodata_value}')
        ds.rio.write_crs(self.crs, inplace=True)

        # Process ones mask option
        ones_array = xr.ones_like(ds[variable0], dtype="int8")
        ones_array = ones_array.rio.write_crs(self.crs, inplace=True)
        ones_array = ones_array.where(ones_array == 1, 0)
        ones_array.rio.write_nodata(0, inplace=True)
        ds["ones_mask"] = ones_array
        ds = ds.drop_vars([v for v in ds.data_vars if v != "ones_mask"])

        return ds

    def perform_shrinkwrap(self):

        self._create_output_folders()
        self.import_and_buffer_polygons()
        ds = self.open_raster_as_ones()

        # Convert to GPD/ Dask GPD
        print(f'Converting xr to gpd...')
        df = ds["ones_mask"].to_dataframe().reset_index()
        gdf = gpd.GeoDataFrame(
            df.ones_mask, geometry=gpd.points_from_xy(df.x, df.y))
        gdf.crs = self.crs
        print(f'Converting to dask...')
        gddf = (dask_geopandas
                .from_geopandas(gdf.set_geometry('geometry'))
                .spatial_shuffle(by="hilbert", npartitions=1000))

        # Fix geoemtry
        print(f'Fixing geometry...{type(gddf["geometry"])}')
        gddf['geometry'] = shapely.make_valid(gddf['geometry'])
        with ProgressBar():
            gdf_out = gddf.compute()

        # Plot / Export
        cplotting.plot_map_data_to_html(gdf_out, self.output_folder, "points")
        gdf_out.to_file(os.path.join(self.vector_folder, "ones_mask.shp"), driver="ESRI Shapefile")
        print(f'Created ones mask\n {gdf_out}')


if __name__ == "__main__":
    input_polygons = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\clipper_01.shp"
    input_grid = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\source_01pct.tif"
    output_folder = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar"
    init = ShrinkwrapRaster(input_polygons, input_grid, output_folder, 0.002, 3418)
    init.perform_shrinkwrap()
