import os
import threading
import geopandas as gpd
import rioxarray as rioxr
from src.d00_utils.regular_grids import create_regular_grid
# from src.d00_utils.system import get_system_memory


class ClipAraster:

    def __init__(self, input_pg_file: [os.path, str], epsg_code: int,
                 raster_value: float, rawraster: [os.path, str],
                 ops_type: str, alternate_outname: str, all_touching: bool):
        self.input_pg_file = input_pg_file
        self.epsg_code = epsg_code
        self.crs = f"EPSG:{epsg_code}"
        self.raster_value = raster_value
        self.raw_raster = rawraster
        self.subtype = ops_type
        if alternate_outname is not None:
            self.alternate_outname = alternate_outname
        else:
            self.alternate_outname = None
        self.alltouched = all_touching

        assert self.subtype in ['make mask', 'clip only', "both"]
        self.chunk_return = {"x": 2048, "y": 2048}
        self._init_outputs()

    def _init_outputs(self):
        base, filename = os.path.split(self.raw_raster)
        name = filename.split(".")[1]
        if not self.alternate_outname:
            self.out_raster = os.path.join(base, f"{name}_CLIPPED.tif")
        else:
            self.out_raster = os.path.join(base, self.alternate_outname)
        self.output_folder = base

    @staticmethod
    def clip_it(ds, gdf, crs, alltouched):
        variable0 = [v for v in ds.data_vars][0]
        nodata_value = ds[variable0].rio.nodata
        ds.rio.write_crs(crs)
        rasterized = ds.rio.clip(gdf.geometry.values, crs=crs, all_touched=alltouched, from_disk=True, drop=False)
        rasterized[variable0].rio.write_nodata(nodata_value, encoded=False, inplace=True)

        return rasterized

    def process_clip(self):
        # Open the raster as dataset
        ds = rioxr.open_rasterio(self.raw_raster, chunks=self.chunk_return,
                                 lock=False, band_as_variable=True)
        print(ds)
        ds = ds.rename_vars({"band_1": "grid_values"})
        variable0 = [v for v in ds.data_vars][0]
        nodata_value = ds[variable0].rio.nodata
        print(f' {variable0} nodata: {nodata_value}')
        ds.rio.write_crs(self.crs, inplace=True)

        # Read Polygon Shapefile
        gdf = gpd.read_file(self.input_pg_file)
        gdf = gdf.to_crs(self.crs)

        # Split it up for processing
        grid_gdf = create_regular_grid(gdf, n_cells=(10, 10), epsg_code=self.epsg_code, overlap=True)
        grid_gdf.to_file(os.path.join(os.path.split(self.input_pg_file)[0], "grid.shp"))
        intersected = grid_gdf.overlay(grid_gdf, how='intersection', keep_geom_type=True)
        intersected = intersected.explode(index_parts=False)
        print(f' Prepped polygons\n {intersected.columns}')

        # Call the clip
        rasterized = self.clip_it(ds, intersected, intersected.crs, alltouched=self.alltouched)
        rasterized[variable0].rio.write_nodata(nodata_value, inplace=True)
        print(f' Created masked array\n{rasterized}')

        print(f'Saving mask grid to disc as {self.out_raster}')
        rasterized.rio.to_raster(self.out_raster, tiled=True, lock=threading.Lock(), compress='LZW', windowed=True,
                                 bigtiff="YES")

        return self.out_raster


if __name__ == "__main__":
    input_shape = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\masks\Mask_0_2pct_03.shp"
    epsg = 3418
    value = 0.01
    raster_file = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\20_DRAFT_DFIRM\DRAFT_DFIRM_WSE_0_2pct.tif"
    operation_type = "clip only"  # ["make mask", "clip only", "make mask"]
    alt_output_filename = "DRAFT_DFIRM_WSE_0_2pct_MASKED.tif"
    exact_or_touched = True

    init = ClipAraster(input_shape, epsg, value, raster_file, operation_type, alt_output_filename, exact_or_touched)
    output_grid_mask = init.process_clip()
    print(f'Output grid mask {output_grid_mask}')
