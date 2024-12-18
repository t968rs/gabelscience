from osgeo import gdal
import os
import typing as T
# from spatialist.auxil import crsConvert
from src.d00_utils.system import increment_file_naming

def gdal_warp(input_files, output_file, target_crs, snap_specs, resampling_method="average"):
    """
    Warp a raster file to a new coordinate system

    Parameters
    ----------
    input_files : list
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
    target_srs = crsConvert(target_crs, 'osr')
    creation_options = ['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=YES', 'NUM_THREADS=ALL_CPUS']
    if snap_specs:
        warp_options = gdal.WarpOptions(format="GTiff",
                                        outputBounds=snap_specs.bounds,
                                        outputBoundsSRS=crsConvert(snap_specs.epsg, 'osr'),
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