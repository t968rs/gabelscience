import rasterio
import os
from rasterio.transform import from_bounds


def export_raster(array, profile, path):
    """
    Export a NumPy array as a raster file using rasterio
    """
    with rasterio.open(path, 'w', **profile) as dst:
        dst.write(array, 1)


def test_export_array(array, folder, line):
    """
    Test export array function
    """
    path = os.path.join(folder, f"test_export_{line}.tif")
    profile = _get_profile()
    export_raster(array, profile, path)


def _get_profile(raster_spec_input):
    """
    Get the raster profile for exporting
    """
    profile = {
        'driver': 'GTiff',
        'dtype': 'float32',
        'nodata': -9999,
        'width': raster_spec_input.width,
        'height': raster_spec_input.height,
        'count': 1,
        'crs': raster_spec_input.crs,
        'transform': from_bounds(*raster_spec_input.bounds, raster_spec_input.width, raster_spec_input.height)
    }
    return profile
