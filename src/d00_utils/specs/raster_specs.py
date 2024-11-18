from dataclasses import dataclass
from rasterio.coords import BoundingBox
from rasterio.transform import Affine
from rasterio.crs import CRS
import xarray as xr
import rioxarray as rioxr
import typing as T
import os


def get_valid_raster_area(path: T.Union[str, os.PathLike, xr.DataArray]) -> float:
    """
    Calculates the area of the dataset in square meters.

    Parameters:
        path:
        ds (xarray.Dataset): The dataset to calculate the area of.

    Returns:
        float: The area of the dataset in square meters.
    """
    if not isinstance(path, xr.DataArray):
        xa = rioxr.open_rasterio(path, chunks={"x": 2048, "y": 2048})
    else:
        xa = path
    d_a = xa.data
    valid_cells = d_a != xa.rio.nodata
    valid_count = valid_cells.sum().compute()
    res = xa.rio.resolution()
    total_area = round(valid_count * abs(res[0]) * abs(res[1]))
    print(f"Total valid raster area: {total_area}")
    return total_area


def get_formatted_file_sizes(path: T.Union[str, os.PathLike] = None,
                             other_size=None) -> T.Tuple[float, float]:
    from src.d00_utils.system import file_size
    if path:
        size_str, size_float, units = file_size(path)
    elif other_size:
        size_str, size_float, units = other_size
    else:
        raise ValueError("Either path or other_size must be provided")
    if units == "GB":
        size_mb = round(size_float * 1024, 0)
        size_gb = round(size_float, 1)
    elif units == "MB":
        size_mb = round(size_float, 1)
        size_gb = round(size_float / 1024, 2)
    elif units == "KB":
        size_mb = round(size_float / 1024, 2)
        size_gb = round(size_float / (1024 * 1024), 3)
    else:
        size_mb = round(size_float / (1024 * 1024), 3)
        size_gb = round(size_float / (1024 * 1024 * 1024), 4)
    return size_mb, size_gb


@dataclass
class RasterSpecs:
    """this is a dataclass that holds the raster specs"""
    res: float
    height: int
    width: int
    epsg: int
    size_mb: float
    size_gb: float
    bounds: tuple
    cellsizes: T.Tuple[float, float]
    path: T.Union[str, object]
    transform: T.Union[str, Affine]
    crs: T.Union[str, object, CRS]
    valid_area: float = None,
    linear_units: str = None

    def __post_init__(self):
        if not isinstance(self.bounds, tuple) or len(self.bounds) != 4:
            raise ValueError(f"Expected bounds to be a tuple of four floats, got {type(self.bounds)}")
        if not (isinstance(self.cellsizes, tuple) and len(self.cellsizes) == 2):
            raise ValueError(f"Expected cellsizes to be a tuple of two floats, got {type(self.cellsizes)}")

    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"'RasterSpecs' object has no attribute '{key}'")

    def __str__(self):
        attrs = self.__dir__()
        values = [getattr(self, attr) for attr in attrs]
        return ', '.join(f"{attr}={value}" for attr, value in zip(attrs, values))

    def __repr__(self):

        attrs = self.__dir__()
        values = [repr(getattr(self, attr)) for attr in attrs]

        repr_str = (f"{self.__class__.__name__:^12}\n\t\t" + '\t\t'
                    .join(f"{attr:>12}: {value:>12}\n" for attr, value in zip(attrs, values)
                          if attr not in ['transform', 'crs', 'bounds']))
        return repr_str + self.crs_repr() + self.bounds_repr() + f"\n\t{self.transform_repr()}"

    def transform_repr(self):
        # Determine the maximum width needed for any value in the transform for proper alignment

        # Format each line with the values aligned according to the maximum width
        # Format each line with the values left-aligned according to the maximum width
        a, b, c = self.transform.a, self.transform.b, self.transform.c
        d, e, f = self.transform.d, self.transform.e, self.transform.f
        for variable in ["a", "b", "c", "d", "e", "f"]:
            val = locals().get(variable, None)
            if val:
                newval = str(val)[:15]
                locals()[variable] = newval

        spacer = " " * 14
        k = "transform"
        endcaps = " | "
        line0 = f"\t{k:>12}: {spacer:>12}"
        line1 = f"{endcaps:<5}{a:<20}, {b:<20}, {c:<20}{endcaps:<20}"
        line2 = f"{endcaps:<5}{d:<20}, {e:<20}, {f:<20}{endcaps:<20}"
        line3 = f"{endcaps:<5}{0:<20}, {0:<20}, {1:<20}{endcaps:<20}"  # Last line is always the same
        return f"{line0}\n{line1}\n{line2}"

    def crs_repr(self):
        k = "crs"
        if self.crs.is_epsg_code:
            v = f"EPSG:{self.crs.to_epsg()}"
            crs_repr = f"\t\t{k:>12}: {v:>12}\n"
        else:
            crs_repr = f"\t{k:>12}: {str(self.crs):>12}"
        return crs_repr

    def bounds_repr(self):
        k = "bounds"
        bounds_parts = str(self.bounds).split(',')
        bp = []
        for p in bounds_parts:
            p = p.strip().replace('(', '').replace(')', '')
            p = p[:15]
            bp.append(p)
        spacer = " " * 14
        bounds_str = (f"\t\t{k:>12}: {spacer:>12}"
                      f"\n{spacer:>20}{bp[0]:>20}, \n{spacer:>20}{bp[1]:>20}, "
                      f"\n{spacer:>20}{bp[2]:>20}, \n{spacer:>20}{bp[3]:>20}")
        return bounds_str

    def as_dict(self):
        ordered_keys = self.__dir__()
        return {key: getattr(self, key) for key in ordered_keys}

    def __dir__(self):
        return [
            'res', 'height', 'width', 'epsg', 'size_mb', 'size_gb', 'bounds',
            'cellsizes', 'path', 'transform', 'crs', 'linear_units', 'valid_area'
        ]


def create_raster_specs_from_path(path, calc_valid_area=False):
    import rasterio

    """
    Create a RasterSpecsObject from a raster file path using rasterio.
    """
    if calc_valid_area:
        valid_area = get_valid_raster_area(path)
    else:
        valid_area = None

    size_mb, size_gb = get_formatted_file_sizes(path)

    with rasterio.open(path) as src:
        bbox = BoundingBox(*src.bounds)
        bbox = (bbox.left, bbox.bottom, bbox.right, bbox.top)

        return RasterSpecs(
            res=round(abs(src.res[0]), 6),  # assuming square pixels
            height=src.height,
            width=src.width,
            epsg=src.crs.to_epsg(),
            size_mb=size_mb,
            size_gb=size_gb,
            bounds=bbox,
            cellsizes=src.res,
            path=path,
            transform=src.transform,
            crs=src.crs,
            linear_units=src.crs.linear_units,
            valid_area=valid_area
        )


def create_raster_specs_from_xarray(array, calc_valid_area=False, **kwargs):
    from src.d00_utils.system import convert_bytes

    if not isinstance(array, xr.DataArray):
        import dask.array as da
        if isinstance(array, da.Array):
            transform = kwargs.get("transform")
            crs = kwargs.get("crs")
            array = dask_to_rioxarray(array, transform=transform, crs=crs)

    if calc_valid_area:
        valid_area = get_valid_raster_area(array)
    else:
        valid_area = None
    array_size = array.nbytes
    size_str = convert_bytes(array_size)
    size, units = size_str.split(" ")
    sizef = float(size)
    size_mb, size_gb = get_formatted_file_sizes(other_size=(size_str, sizef, units))

    return RasterSpecs(
        res=round(abs(array.rio.resolution()[0]), 6),  # assuming square pixels
        height=array.rio.height,
        width=array.rio.width,
        epsg=array.rio.crs.to_epsg(),
        size_mb=size_mb,
        size_gb=size_gb,
        bounds=array.rio.bounds(),
        cellsizes=array.rio.resolution(),
        path=None,
        transform=array.rio.transform(),
        crs=array.rio.crs,
        valid_area=valid_area
    )


def dask_to_rioxarray(array, transform, crs):
    """
    Convert a Dask array to a rioxarray DataArray with proper coordinates and dimensions.
    """
    import numpy as np

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