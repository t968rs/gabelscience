"""Module for exporting raster data to tif files"""
import typing as T
import os
import threading
import dask.array as da
import numpy as np
import xarray as xr
from src.d01_processing.raster_ops import numpy_to_rioxarray, dask_to_rioxarray


def test_export_array(array, folder: str, line: int, nodata_write=None, **kwargs):
    """Export an array to a tif file for testing purposes"""

    if isinstance(array, np.ndarray):
        if "profile" not in kwargs:
            raise ValueError("Profile is required for NumPy arrays")
        array = numpy_to_rioxarray(array, kwargs.get("profile"))

    if isinstance(array, da.Array):
        array = xr.DataArray(array, dims=["y", "x"])

    if nodata_write is None:
        nodata_write = array.rio.nodata if isinstance(array, xr.DataArray) else None

    # Export file naming
    test_folder = os.path.join(folder)
    if not os.path.exists(test_folder):
        os.makedirs(test_folder)

    if isinstance(array, T.Union[xr.DataArray, da.Array]):
        name = array.name if array.name else "TEST"
    else:
        name = [v for v in array.data_vars.keys()][0]
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
            if not nodata_write:
                nodata_write = array.rio.nodata
            array.rio.write_nodata(nodata_write, inplace=True, encoded=False)
            array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                windowed=True, bigtiff="YES")
    else:
        array.rio.write_nodata(nodata_write, inplace=True, encoded=False)
        array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                            windowed=True, bigtiff="YES")
    print(f"TEST EXPORT PATH: {outpath}")
    # print(f'TEST EXPORT: \n --- \n {array}\n')


def export_raster(array: [xr.DataArray, xr.Dataset],
                  outpath: T.Union[os.PathLike, str],
                  nodata: T.Union[float, int] = None, **kwargs) -> T.Union[os.PathLike, str]:
    """Export an array to a tif file"""

    # Test for NumPy array
    if isinstance(array, np.ndarray):
        if "profile" not in kwargs:
            raise ValueError("Profile is required for NumPy arrays")
        array = numpy_to_rioxarray(array, kwargs.get("profile"))
    elif isinstance(array, da.Array):
        array = dask_to_rioxarray(array, transform=kwargs.get("transform"), crs=kwargs.get("crs"))

    if isinstance(array, xr.Dataset):
        ds = array
        for varname in array.rio.vars:
            array = ds[varname]
            if array.dtype == "float32":
                nodata = -9999
            else:
                nodata = array.rio.nodata
            array.rio.write_nodata(nodata, inplace=True, encoded=False)

            # Write it out
            try:
                array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                    windowed=True, bigtiff="YES")
            except Exception as e:
                print(f"ERROR LN 66: {e}")
                os.remove(outpath)
    else:
        if array.dtype in ["float32", np.float32]:
            nodata = -9999
        else:
            nodata = array.rio.nodata
        array.rio.write_nodata(-9999, inplace=True, encoded=False)

        # Write it out
        try:
            crs = kwargs.get("crs", None)
            if crs:
                array.rio.write_crs(crs, inplace=True)
            array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                windowed=True, bigtiff="YES")
        except Exception as e:
            print(f"ERROR LN 76: {e}")
            os.remove(outpath)

    print(f"\tEXPORT PATH: {outpath}, \n\t NODATA: {nodata}")
    # print(f'EXPORT: \n --- \n {array}\n')
    return outpath

