import os
import subprocess
import tempfile

import os
import tempfile
from osgeo import gdal

from pyproj import CRS
from typing import List, Union

# Your helper functions
from src.d01_processing.gdal_helpers import create_vrt, gdal_warp



class RasterMBTilesBuilder:
    """
    A helper class to merge multiple .tif files (via your helper functions)
    into a single MBTiles raster using the GDAL Python API (no CLI needed).
    """

    def __init__(self,
                 min_zoom: int = 0,
                 max_zoom: int = 12,
                 tile_format: str = "PNG",
                 apply_warp: bool = False,
                 target_crs: Union[str, int] = None,
                 snap_specs=None):
        """
        Parameters
        ----------
        min_zoom : int
            Minimum zoom level for the output MBTiles.
        max_zoom : int
            Maximum zoom level for the output MBTiles.
        tile_format : str
            Tile image format: e.g. "PNG" or "JPEG".
        apply_warp : bool
            Whether to call your gdal_warp function to reproject the mosaic
            into the target_crs.
        target_crs : str or int
            EPSG code or PROJ string for output. e.g. 'EPSG:3857'.
        snap_specs : object
            An object containing raster specs like bounds, cell sizes, etc.
            Passed to gdal_warp if apply_warp = True.
        """
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.tile_format = tile_format  # "PNG" or "JPEG"
        self.apply_warp = apply_warp
        self.target_crs = target_crs
        self.snap_specs = snap_specs

    def create_mbtiles(self,
                       tif_files: List[str],
                       output_mbtiles: str,
                       remove_temp_files: bool = True) -> str:
        """
        Merge multiple .tif files into a single MBTiles file with a Google
        (Spherical Mercator) tiling scheme, using your helper functions.

        Parameters
        ----------
        tif_files : list
            List of paths to the input .tif files.
        output_mbtiles : str
            Path to the final MBTiles output.
        remove_temp_files : bool
            Whether to remove any intermediate files.

        Returns
        -------
        str
            The path to the generated MBTiles file.
        """
        if not tif_files:
            raise ValueError("No .tif files provided to merge.")

        # Create a temp folder for intermediate outputs
        tempdir = tempfile.mkdtemp(prefix="mbtiles_")

        # 1) Create a VRT mosaic from the list of TIFs using your helper.
        vrt_path = os.path.join(tempdir, "merged_raster.vrt")
        print(f"[INFO] Creating VRT: {vrt_path}")
        create_vrt(tif_files, vrt_path)

        # 2) Optionally warp the VRT to a target CRS or snap specs
        if self.apply_warp and self.target_crs:
            warped_path = os.path.join(tempdir, "merged_raster_warped.tif")
            print(f"[INFO] Warping VRT to {self.target_crs}: {warped_path}")
            # Call your gdal_warp helper
            # The helper can handle multiple input files or a single file
            # so we pass [vrt_path] in a list
            gdal_warp([vrt_path],
                      warped_path,
                      target_crs=self.target_crs,
                      snap_specs=self.snap_specs,
                      resampling_method="NearestNeighbor")
            source_raster = warped_path
        else:
            # If no warp, we'll just use the VRT as is.
            source_raster = vrt_path

        # 3) Convert the mosaic to MBTiles using the GDAL MBTiles driver
        #    We'll specify TILING_SCHEME=Google to get Web Mercator tiling.
        print(f"[INFO] Converting to MBTiles: {output_mbtiles}")
        src_ds = gdal.Open(source_raster, gdal.GA_ReadOnly)
        if not src_ds:
            raise RuntimeError(f"Failed to open source raster: {source_raster}")

        # We use CreateCopy with the MBTiles driver
        # TILING_SCHEME=Google ensures a typical web-mercator style pyramid
        # TILE_FORMAT=PNG or JPEG
        driver = gdal.GetDriverByName("MBTILES")
        mbt_ds = driver.CreateCopy(
            output_mbtiles,
            src_ds,
            0,  # strict=0
            options=[
                "TILING_SCHEME=Google",
                f"TILE_FORMAT={self.tile_format}"
            ]
        )
        mbt_ds.FlushCache()

        # 4) Build overviews for zoom levels
        # The standard gdaladdo approach is to define overview levels as integer scale factors.
        # For a typical web-mercator tile pyramid, each higher zoom is about x2 resolution.
        # So if min_zoom=0, max_zoom=12 => we create overviews for levels 2^1, 2^2, ..., 2^12
        overview_list = []
        for z in range(self.min_zoom + 1, self.max_zoom + 1):
            # Each gdal overview level is a scale factor.
            # E.g. "2" => 2x, "4" => 4x, "8" => 8x, etc.
            overview_list.append(int(2 ** (z - self.min_zoom)))

        if overview_list:
            print(f"[INFO] Building overviews: {overview_list}")
            # BuildOverviews() requires a resampling method, e.g. 'NEAREST', 'AVERAGE', ...
            mbt_ds.BuildOverviews("AVERAGE", overview_list)
            mbt_ds.FlushCache()

        # Clean up
        mbt_ds = None
        src_ds = None

        # 5) Remove intermediate files if requested
        if remove_temp_files:
            try:
                os.remove(vrt_path)
            except OSError:
                pass
            if self.apply_warp and os.path.exists(source_raster):
                try:
                    os.remove(source_raster)
                except OSError:
                    pass
            try:
                os.rmdir(tempdir)
            except OSError:
                pass

        print(f"[DONE] MBTiles created at: {output_mbtiles}")
        return output_mbtiles


if __name__ == "__main__":

    tif_list = []
    input_folder = r"Z:\Shell_Rock\02_WORKING\01_RAW_Mosaic"
    for f in os.listdir(input_folder):
        if f.endswith(".tif"):
            tif_list.append(os.path.join(input_folder, f))


    output_mbtiles = r"Z:\Shell_Rock\03_delivery\02_internal_viewer\rasters"

    # Instantiate the builder
    builder = RasterMBTilesBuilder(min_zoom=3, max_zoom=12, tile_format="PNG",
                                   apply_warp=True, target_crs=4326,)

    # Create MBTiles
    mbtiles_path = builder.create_mbtiles(tif_list, output_mbtiles)

    # Now, you can upload `final_raster.mbtiles` to Mapbox using the Uploads API or Studio.