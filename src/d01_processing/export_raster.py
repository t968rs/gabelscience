"""Module for exporting raster data to tif files"""
import typing as T
import os
import threading
import xarray as xr


def test_export_array(array, folder: str, line):
    """Export an array to a tif file for testing purposes"""

    # Export file naming
    test_folder = os.path.join(folder)
    if not os.path.exists(test_folder):
        os.makedirs(test_folder)

    if isinstance(array, xr.DataArray):
        name = array.name
    else:
        name = [v for v in array.rio.vars][0]
    if name:
        outname = f"{name}_TEST_{line}"
    else:
        outname = f"TEST_{line}"
    outpath = os.path.join(test_folder, outname + ".tif")
    if os.path.exists(outpath):
        for i in range(20):
            path = os.path.join(test_folder, f"{outname}_{i}.tif")
            if not os.path.exists(path):
                outpath = path
                break
    if isinstance(array, xr.Dataset):
        ds = array
        for varname in array.rio.vars:
            array = ds[varname]
            array.rio.write_nodata(array.rio.nodata, inplace=True, encoded=False)
            array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                windowed=True, bigtiff="YES")
    else:
        array.rio.write_nodata(array.rio.nodata, inplace=True, encoded=False)
        array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                            windowed=True, bigtiff="YES")
    print(f"TEST EXPORT PATH: {outpath}")
    print(f'TEST EXPORT: \n --- \n {array}\n')


def export_raster(array: [xr.DataArray, xr.Dataset],
                  outpath: T.Union[os.PathLike, str],
                  nodata: T.Union[float, int] = None):
    """Export an array to a tif file"""

    if isinstance(array, xr.Dataset):
        ds = array
        for varname in array.rio.vars:
            array = ds[varname]
            array.rio.write_nodata(array.rio.nodata, inplace=True, encoded=False)
            array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                windowed=True, bigtiff="YES")
    else:
        array.rio.write_nodata(array.rio.nodata, inplace=True, encoded=False)
        array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                            windowed=True, bigtiff="YES")
    print(f"EXPORT PATH: {outpath}")
    print(f'EXPORT: \n --- \n {array}\n')
    return outpath
