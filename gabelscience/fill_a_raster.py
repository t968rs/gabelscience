import contextlib
import os
import sys
import threading
from time import sleep
from time import time

import dask.config
import numpy as np
import rasterio
import rasterio.warp
import rasterio.windows
import rioxarray as rioxr
import xarray as xr
from dask.distributed import Client, LocalCluster
from rasterio.crs import CRS
from tqdm import tqdm

from src.d00_utils import maths, regular_grids
from src.d00_utils.bounds_convert import bbox_to_gdf
from src.d00_utils.system import get_system_memory, file_size
from src.d03_show.printers import print_attributes
from src.specs.raster_specs import create_raster_specs_from_path
from src.d00_utils.check_crs_from_pathlist import check_crs_match_from_list
from src.d01_processing.raster_ops import *


class FillRasterWith:
    def __init__(self, raster_path, fill_raster_path, valid_cells_raster_path, output_path=None):
        self.raster_path = raster_path
        self.fill_raster = fill_raster_path
        self.valid_raster = valid_cells_raster_path
        self.output_path = output_path
        self.raster_specs = create_raster_specs_from_path(raster_path)
        self.crs_input_match = self._check_crs_match()

    def _check_crs_match(self):
        return check_crs_match_from_list([self.fill_raster, self.valid_raster, self.raster_path])

    def open_rasters_to_ds(self):
        chunk_size = {"x": 2048, "y": 2048}
        # Open valid raster first
        valid = rioxr.open_rasterio(self.valid_raster).rename("valid_raster")
        valid_extent = valid.rio.bounds()
        print(f' Valid Raster: {valid_extent}')

        # Create a mask of valid cells
        print(f' Creating mask of valid cells...')
        valid = mask_with_ones(valid).chunk(chunk_size)

        # Open the other rasters
        print(f' Opening input raster...')
        raster = (rioxr.open_rasterio(self.raster_path).rename("input_raster")
                  .rio.clip_box(*valid_extent).chunk(chunk_size))

        print(f' Opening filler raster...')
        filler = (rioxr.open_rasterio(self.fill_raster).rename("fill_raster")
                  .rio.clip_box(*valid_extent).chunk(chunk_size))

        print(f"Combinging rasters: {valid.shape}, {raster.shape}, {filler.shape}")
        ds = xr.combine_by_coords([valid, raster, filler],
                                  join="left",
                                  combine_attrs="override")

        return ds

    def fill_raster_with_another(self, input_str, fill_str, valid_str):
        ds = self.open_rasters_to_ds()

        # Get the filler raster
        filler = ds[fill_str].where(ds[valid_str] == 1)

        # Fill the raster
        filled_raster = xr.where(ds[valid_str] == 1, ds[input_str], filler)
        return filled_raster.chunk({"x": 2048, "y": 2048})


def fill_and_export_raster(raster_path, fill_raster_path, valid_cells_raster_path, output_path=None):
    fr = FillRasterWith(raster_path, fill_raster_path, valid_cells_raster_path, output_path)
    filled_raster = fr.fill_raster_with_another("input_raster", "fill_raster", "valid_raster")

    # Naming
    if output_path:
        outpath = output_path
        if os.path.isdir(output_path):
            name = os.path.basename(raster_path)
            if "." in name:
                name = name.split(".")[0]
            name = f"{name}_filled.tif"
            outpath = os.path.join(output_path, name)
    else:
        outpath = raster_path
        if "." in outpath:
            outpath = outpath.split(".")[0] + "_filled.tif"
        else:
            outpath = outpath + "_filled.tif"

    # Export the raster
    print(f"Filled Raster: {filled_raster}")
    print(f"Exporting filled raster to: {outpath}")
    export_raster(filled_raster, outpath)
    if os.path.exists(outpath):
        print(f" Exported filled raster to: {outpath}")


if __name__ == "__main__":
    in_raster = r"E:\Iowa_1A\02_mapping\Grids_CopperasDuck\02_FillMask\Depth_04pct.tif"
    fill_raster = r"E:\Iowa_1A\02_mapping\Grids_CopperasDuck\terrain_FT4.tif"
    valid_raster = r"E:\Iowa_1A\02_mapping\Grids_CopperasDuck\02_FillMask\WSE_FRP\WSE_0_2pct.tif"
    outputpath = r"E:\Iowa_1A\02_mapping\Grids_CopperasDuck\02_FillMask\test_fill"

    fill_and_export_raster(in_raster, fill_raster, valid_raster, outputpath)
