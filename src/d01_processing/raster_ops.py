import xarray as xr
import rioxarray as rioxr
import numpy as np
import threading
import dask.array as da
import subprocess



def mask_with_ones(input_ds, key_string=None) -> xr.DataArray:
    # Pre-process input data
    # print(f"{input_ds.data_vars.keys()}")
    if isinstance(input_ds, xr.Dataset):
        if key_string is None:
            raise ValueError("key_string must be provided if input is a Dataset")
        varname = [k for k in input_ds.data_vars.keys() if key_string in k][0]
        input_array = input_ds[varname]
    else:
        input_array = input_ds
        varname = input_array.name

    print(f'\n  No Data: {input_array.rio.nodata}')
    print(f'  Encoded: {input_array.rio.encoded_nodata}')
    if not input_array.rio.nodata:
        input_array.rio.write_nodata(input_array.rio.encoded_nodata, encoded=False, inplace=True)
    print(f'  No Data: {input_array.rio.nodata}')
    encoded = input_array.rio.encoded_nodata
    non_encoded = input_array.rio.nodata
    print(f'  Encoded: {encoded}\n')

    # Make the shaped-array
    ones_array = xr.where(input_array == non_encoded, np.nan, 1).astype('float32').rename(f'{varname}_mask')
    ones_array = xr.where(ones_array == 1, 1, -1).astype('int8')

    input_array.fillna(-1)
    input_array.rio.write_nodata(-1, inplace=True, encoded=False)
    input_array.rio.set_nodata(-1, inplace=True)
    print(f' Ones mask array: {ones_array}')
    return input_array


def numpy_to_rioxarray(array, profile):
    """
    Convert a NumPy array to a rioxarray DataArray with proper coordinates and dimensions.
    """
    # Extract the transform and CRS from the profile
    print("\tInput is NumPy array")
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


def dask_to_rioxarray(array, transform, crs):
    """
    Convert a Dask array to a rioxarray DataArray with proper coordinates and dimensions.
    """

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


def calculate_statistics_and_overviews(raster_path):
    # Build Overviews (Pyramids) using "nearest" resampling algorithm
    subprocess.run(['gdaladdo', '--config', 'GDAL_TIFF_OVR_BLOCKSIZE', 'all', '-r', 'nearest', raster_path])

    # Compute Statistics
    subprocess.run(['gdalinfo', '-stats', raster_path])

