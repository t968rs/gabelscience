import xarray as xr
import rioxarray as rioxr
import numpy as np
import threading


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


def export_raster(array, out_path):
    array.rio.to_raster(out_path, tiled=True, lock=threading.Lock(), compress='LZW',
                        windowed=True, bigtiff="YES")
    return out_path
