from osgeo import gdal
from osgeo.osr import SpatialReference as SR
from pyproj import CRS, Transformer
import os
import typing as T
from src.d00_utils.system import increment_file_naming


def create_vrt(input_files: T.List, vrt_path: T.Union[str, os.PathLike]) -> T.Union[str, os.PathLike]:
    gdal.BuildVRT(vrt_path, input_files,
                  allowProjectionDifference=False,
                  separate=True,
                  resampleAlg='bilinear',
                  VRTNodata=-9999,
                  resolution='highest',)
    return vrt_path



def gdal_warp(input_files, output_file, target_crs, snap_specs, resampling_method="average"):
    """
    Warp a raster file to a new coordinate system

    Parameters
    ----------
    input_files : T.Union[str, T.List[str]]
        Paths to the input files
    output_file : str
        Path to the output file
    target_crs : T.Union[str, int]
        Target coordinate system
    snap_specs : RasterSpecs
        Raster specs to snap the output to
    resampling_method : str
        Resampling method


    Returns
    -------
    None
    """
    if isinstance(input_files, str):
        input_files = [input_files]


    # Import CRS object
    target_crs = CRS.from_user_input(target_crs)
    target_epsg = target_crs.to_epsg()

    # create SR object
    target_srs = SR()

    target_srs.ImportFromEPSG(target_epsg)

    creation_options = ['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=YES', 'NUM_THREADS=ALL_CPUS']
    if snap_specs:
        warp_options = gdal.WarpOptions(format="GTiff",
                                        outputBounds=snap_specs.bounds,
                                        outputBoundsSRS=target_srs,
                                        xRes=snap_specs.cellsizes[0],
                                        yRes=snap_specs.cellsizes[1],
                                        targetAlignedPixels=True,
                                        warpMemoryLimit=1024,
                                        resampleAlg=resampling_method,
                                        dstNodata=-9999,
                                        multithread=True,
                                        creationOptions=creation_options)
        gdal.Warp(output_file, input_files, dstSRS=target_srs, options=warp_options)
    else:
        outbase = os.path.split(output_file)[0]

        out_vrt = increment_file_naming(os.path.join(outbase, 'temp.vrt'))

        vrt = gdal.BuildVRT(out_vrt, input_files)
        gdal.Warp(output_file, vrt, dstSRS=target_srs, resampleAlg='bilinear',
                           multithread=True,
                           creationOptions=creation_options)

    return output_file