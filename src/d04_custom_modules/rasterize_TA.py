import geopandas as gpd
import numpy as np
import rasterio.features
import typing as T
import pandas as pd
import odc.geo.geobox
from numpy.typing import NDArray
from rasterio.enums import MergeAlg
from src.specs import _dtypes


def _rasterize_target_aligned(
        geometry_array: gpd.GeoSeries,
        data_values: T.Union[NDArray, pd.arrays.IntegerArray],
        geobox: odc.geo.geobox.GeoBox,
        fill: float,
        merge_alg: MergeAlg = MergeAlg.replace,
        filter_nan: bool = False, default_value: int = 1, ):
    """
    Custom rasterization function that includes target_aligned_pixels.
    This function is a wrapper around rasterio.features.rasterize, adding the target_aligned_pixels argument.
    geometry_array: geopandas.GeoSeries,
    data_values: Union[NDArray, pandas.arrays.IntegerArray],
    geobox: odc.geo.geobox.GeoBox,
    fill: float,
    merge_alg: MergeAlg = MergeAlg.replace,
    filter_nan: bool = False,
    all_touched: bool = False,
    ** ignored_kwargs
    ->
    Optional[NDArray]:
    Rasterize a list of shapes+values for a given GeoBox.

    Parameters
    -----------
    geometry_array: geopandas.GeoSeries
        A geometry array of points.
    data_values: Union[NDArray, pandas.arrays.IntegerArray]
        Data values associated with the list of geojson shapes
    geobox: :obj:`odc.geo.geobox.GeoBox`
        Transform of the resulting image.
    fill: float
        The value to fill in the grid with for nodata.
    merge_alg: `rasterio.enums.MergeAlg`, optional
        The algorithm for merging values into one cell. Default is `MergeAlg.replace`.
    filter_nan: bool, optional
        If True, will remove nodata values from the data before rasterization.
        Default is False.


    Returns
    -------
    :obj:`numpy.ndarray` or None
        The vector data in the rasterized format.

    """

    if isinstance(data_values, pd.arrays.IntegerArray):
        data_values = data_values.to_numpy(
            dtype=_dtypes.minimize_dtype(data_values.dtype.numpy_dtype, fill),
            na_value=fill)

    if filter_nan:
        data_values, geometry_array = _dtypes.remove_missing_data(data_values, geometry_array)

    image = rasterio.features.rasterize(
        zip(geometry_array.values, data_values),
        out_shape=(geobox.height, geobox.width),
        transform=geobox.affine,
        fill=fill,
        all_touched=True,
        merge_alg=merge_alg,
        dtype=_dtypes.minimize_dtype(data_values.dtype, fill),
        default_value=default_value
    )
    return image
