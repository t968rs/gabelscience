import numpy as np
import geopandas as gpd
import numpy
import odc.geo.geobox
import pandas as pd
import rasterio

from numpy.typing import NDArray
from packaging import version
from rasterio.enums import MergeAlg
from scipy.interpolate import Rbf, griddata
import typing as T

_INT8_SUPPORTED = version.parse(rasterio.__gdal_version__) >= version.parse(
    "3.7.0"
) and version.parse(rasterio.__version__) >= version.parse("1.3.7")


def is_numeric(data_values: NDArray) -> bool:
    """
    Check if array data type is numeric.
    """
    return np.issubdtype(data_values.dtype.type, np.number)


def remove_missing_data(
    data_values: NDArray,
    geometry_array: gpd.GeoSeries,
) -> tuple[NDArray, gpd.GeoSeries]:
    """
    Missing data causes issues with interpolation of point data
    https://github.com/corteva/geocube/issues/9

    This filters the data so those issues don't cause problems.
    """
    not_missing_data = ~pd.isnull(data_values)
    geometry_array = geometry_array[not_missing_data]
    data_values = data_values[not_missing_data]
    return data_values, geometry_array


def minimize_dtype(dtype: numpy.dtype, fill: float) -> numpy.dtype:
    """
    If int64, convert to float64:
    https://github.com/OSGeo/gdal/issues/3325

    Attempt to convert to float32 if fill is NaN and dtype is integer.
    """
    if numpy.issubdtype(dtype, numpy.integer):
        if not _INT8_SUPPORTED and dtype.name == "int8":
            # GDAL<3.7/rasterio<1.3.7 doesn't support int8
            dtype = numpy.dtype("int16")
        if numpy.isnan(fill):
            dtype = (
                numpy.dtype("float64") if dtype.itemsize > 2 else numpy.dtype("float32")  # type: ignore
            )
    elif not numpy.issubdtype(dtype, numpy.floating):
        # default to float64 for non-integer/float types
        dtype = numpy.dtype("float64")
    return dtype

