from dataclasses import dataclass

int_types = ["res", "height", "width", "epsg"]
float_types = ["res", "size_mb", "size_gb"]
tuple_types = ["bounds", "cellsizes"]
objectandstr_types = ["path", "transform", "crs"]
all_types = int_types + float_types + tuple_types + objectandstr_types

ExpectedTypes = {t: [] for t in all_types}


def _init_expected_types():
    for t in int_types:
        ExpectedTypes[t].append(int)
    for t in float_types:
        ExpectedTypes[t].append(float)
    for t in tuple_types:
        ExpectedTypes[t].append(tuple)
    for t in objectandstr_types:
        ExpectedTypes[t].extend([str, object])
    ExpectedTypes["cellsizes"] = [tuple]


@dataclass
class RasterSpecs:
    """this is a dataclass that holds the raster specs"""
    res: [float, int]
    height: int
    width: int
    epsg: int
    size_mb: round(float)
    size_gb: round(float, 1)
    bounds: tuple
    cellsizes: tuple[float, float]
    path: [str, object]
    transform: [str, object]
    crs: [str, object]

    def __init__(self, kwargs):
        if not isinstance(kwargs, dict):
            raise ValueError(f"Expected dictionary, got {type(kwargs)}")

        _init_expected_types()
        self._type_checks(kwargs)

    @staticmethod
    def _type_checks(kwargs):
        errors = []
        for key, exp_typ in ExpectedTypes.items():
            if key in kwargs:
                if key == "cellsizes":
                    if not (isinstance(kwargs[key], tuple) and len(kwargs[key]) == 2 and all(
                            isinstance(i, float) for i in kwargs[key])):
                        errors.append(f"Expected a tuple of two floats for {key}, got {kwargs[key]}")
                elif not isinstance(kwargs[key], tuple(exp_typ)):
                    errors.append(f"Expected {exp_typ} for {key}, got {type(kwargs[key])}")
            else:
                errors.append(f"Missing key: {key}")

        if errors:
            raise ValueError("Errors encountered during initialization:\n" + "\n".join(errors))


def create_raster_specs_from_path(path):
    import rasterio
    from src.d00_utils import file_size
    """
    Create a RasterSpecsObject from a raster file path using rasterio.
    """

    size_str, size_float, units = file_size.file_size(path)
    if units == "GB":
        size_mb = size_float * 1024
        size_gb = size_float
    elif units == "MB":
        size_mb = size_float
        size_gb = size_float / 1024
    elif units == "KB":
        size_mb = size_float / 1024
        size_gb = size_float / (1024 * 1024)
    else:
        size_mb = size_float / (1024 * 1024)
        size_gb = size_float / (1024 * 1024 * 1024)

    with rasterio.open(path) as src:
        kwargs = {
            'res': round(abs(src.res[0]), 6),  # assuming square pixels
            'height': src.height,
            'width': src.width,
            'epsg': src.crs.to_epsg(),
            'size_mb': size_mb,
            'size_gb': size_gb,
            'bounds': src.bounds,
            'cellsizes': src.res,
            'path': path,
            'transform': src.transform,
            'crs': src.crs,
            'epsg_code': src.crs.to_epsg(),
        }

        return RasterSpecs(kwargs)
