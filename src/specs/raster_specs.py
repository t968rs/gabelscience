from dataclasses import dataclass
import typing as T
from rasterio.coords import BoundingBox
from rasterio.transform import Affine


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
    crs: T.Union[str, object]

    def __post_init__(self):
        if not isinstance(self.bounds, tuple) or len(self.bounds) != 4:
            raise ValueError(f"Expected bounds to be a tuple of four floats, got {type(self.bounds)}")
        if not (isinstance(self.cellsizes, tuple) and len(self.cellsizes) == 2):
            raise ValueError(f"Expected cellsizes to be a tuple of two floats, got {type(self.cellsizes)}")

    def __str__(self):
        attrs = self.__dir__()
        values = [getattr(self, attr) for attr in attrs]
        return ', '.join(f"{attr}={value}" for attr, value in zip(attrs, values))

    def __repr__(self):

        attrs = self.__dir__()
        values = [repr(getattr(self, attr)) for attr in attrs]
        transform_repr = self.transform_str()
        repr_str = (f"{self.__class__.__name__}\n" + ' '
                    .join(f"{attr}: {value}\n" for attr, value in zip(attrs, values) if attr != 'transform'))
        return repr_str + f"{transform_repr}"

    def transform_str(self):
        # Determine the maximum width needed for any value in the transform for proper alignment
        max_width = max(len(f"{getattr(self.transform, attr)}") for attr in ['a', 'b', 'c', 'd', 'e', 'f'])
        max_width = int(round(max_width * 0.75))

        # Format each line with the values aligned according to the maximum width
        # Format each line with the values left-aligned according to the maximum width
        line0 = "transform:"
        line1 = (f"\t| {self.transform.a:<{max_width}}, {self.transform.b:<{max_width}}, "
                 f"{round(self.transform.c, 10):<{max_width}} |")
        line2 = (f"\t| {self.transform.d:<{max_width}}, {self.transform.e:<{max_width}}, "
                 f"{round(self.transform.f, 10):<{max_width}} |")
        line3 = f"\t| {0:<{max_width}}, {0:<{max_width}}, {1:<{max_width}} |"  # Last line is always the same
        return f"{line0}\n{line1}\n{line2}"

    def as_dict(self):
        ordered_keys = self.__dir__()
        return {key: getattr(self, key) for key in ordered_keys}

    def __dir__(self):
        return [
            'res', 'height', 'width', 'epsg', 'size_mb', 'size_gb', 'bounds',
            'cellsizes', 'path', 'transform', 'crs'
        ]


def create_raster_specs_from_path(path):
    import rasterio
    from src.d00_utils.system import file_size
    """
    Create a RasterSpecsObject from a raster file path using rasterio.
    """

    size_str, size_float, units = file_size(path)
    if units == "GB":
        size_mb = round(size_float * 1024, 0)
        size_gb = round(size_float, 1)
    elif units == "MB":
        size_mb = round(size_float, 0)
        size_gb = round(size_float / 1024, 1)
    elif units == "KB":
        size_mb = round(size_float / 1024, 0)
        size_gb = round(size_float / (1024 * 1024), 1)
    else:
        size_mb = round(size_float / (1024 * 1024), 0)
        size_gb = round(size_float / (1024 * 1024 * 1024), 1)
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
        )
