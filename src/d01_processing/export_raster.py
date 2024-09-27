"""Module for exporting raster data to tif files"""
import typing as T
import os
import threading

import numpy as np
import xarray as xr


def test_export_array(array, folder: str, line: int, nodata_write=None, **kwargs):
    """Export an array to a tif file for testing purposes"""

    if isinstance(array, np.ndarray):
        if "profile" not in kwargs:
            raise ValueError("Profile is required for NumPy arrays")
        array = numpy_to_rioxarray(array, kwargs.get("profile"))

    if nodata_write is None:
        nodata_write = array.rio.nodata if isinstance(array, xr.DataArray) else None

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

    """Export an array to a tif file for testing purposes"""

    # Test for NumPy array
    if isinstance(array, np.ndarray):
        if "profile" not in kwargs:
            raise ValueError("Profile is required for NumPy arrays")
    array = numpy_to_rioxarray(array, kwargs.get("profile"))

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
        array.rio.write_nodata(nodata, inplace=True, encoded=False)

        # Write it out
        try:
            array.rio.to_raster(outpath, tiled=True, lock=threading.Lock(), compress='LZW',
                                windowed=True, bigtiff="YES")
        except Exception as e:
            print(f"ERROR LN 76: {e}")
            os.remove(outpath)

    print(f"EXPORT PATH: {outpath}")
    # print(f'EXPORT: \n --- \n {array}\n')
    return outpath


def numpy_to_rioxarray(array, profile):
    """
    Convert a NumPy array to a rioxarray DataArray with proper coordinates and dimensions.
    """
    # Extract the transform and CRS from the profile
    transform = profile['transform']
    crs = profile['crs']

    # Create coordinate arrays for 'x' and 'y'
    y_coords = np.arange(array.shape[0]) * transform[4] + transform[5]
    x_coords = np.arange(array.shape[1]) * transform[0] + transform[2]

    # Create the DataArray with the correct dimensions and coordinates
    data_array = xr.DataArray(
        array,
        dims=['y', 'x'],
        coords={'y': y_coords, 'x': x_coords}
    )

    # Assign the CRS and transform to the DataArray
    data_array = data_array.rio.write_crs(crs)
    data_array = data_array.rio.write_transform(transform)

    return data_array.chunk({"x": 2048, "y": 2048})
