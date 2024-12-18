from typing import List
from dataclasses import dataclass, field
import os
from src.d00_utils.loggers import getalogger


# sits in src/d00_utils/file_typing.py
"""Library and methods for getting file extensions and categorizing files"""

# Extension lookup dictionary
EXT_LOOKUP = {
    "shp": ["Shapefile", ".shp", "shp"],
    "geojson": ["GeoJSON", ".geojson", "geojson"],
    "gdb": ["FileGDB", ".gdb", "gdb"],
    "tif": ["TIFF", ".tif", "tif"],
    "img": ["IMG", ".img", "img"],
    "json": ["JSON", ".json", "json"],
    "gpkg": ["GPKG", ".gpkg", "gpkg"],
    "csv": ["CSV", ".csv", "csv"],
    "xlsx": ["Excel", ".xlsx", "xlsx"],
    "dbf": ["DBF", ".dbf", "dbf"],
}

FTYPES = {"fc": "GDB Feature Class",
              "glyr": "GeoPackage Layer",
              "gtiff": "GeoTIFF Raster",
              "img": "ERDAS Raster",
              "json": "JSON",
              "geojson": "GeoJSON",
              "xlsx": "Excel",
              "dbf": "DBF"}

FTYP_EXTS = {"fc": ".gdb",
                        "glyr": ".gpkg",
                        "gtiff": ".tif",
                        "img": ".img",
                        "json": ".json",
                        "geojson": ".geojson",
                        "xlsx": ".xlsx",
                        "dbf": ".dbf"}

# Spatial files lookup dictionary
SPATIAL_TYPES_LOOKUP = {
    "vector": [".shp", ".geojson", ".gdb", ".gpkg"],
    "raster": [".tif", ".img"]
}

# Table files lookup dictionary
TABLE_TYPES_LOOKUP = {
    "csv": [".csv"],
    "xlsx": [".xlsx"],
    "dbf": [".dbf"],
    "json": [".json"]
}

CATEGORIES = {"vector", "raster", "table"}

streamlog = getalogger("file_typing", level="WARNING")
flogger = getalogger("file_typing", level=10)

@dataclass
class FileTypeInfo:
    code: str            # e.g., 'fc', 'glyr'
    description: str     # e.g., 'GDB Feature Class', 'GeoPackage Layer'
    extensions: List[str]  # e.g., ['.gdb'], ['.gpkg']
    category: str        # e.g., 'vector', 'raster', 'table'
    has_db: bool         # Whether this file type is a database

FILE_TYPES = [
    FileTypeInfo(code='fc', description='GDB Feature Class', extensions=['.gdb'], category='vector', has_db=True),
    FileTypeInfo(code='shp', description='Shapefile', extensions=['.shp'], category='vector', has_db=False),
    FileTypeInfo(code='glyr', description='GeoPackage Layer', extensions=['.gpkg'], category='vector', has_db=True),
    FileTypeInfo(code='gtiff', description='GeoTIFF Raster', extensions=['.tif'], category='raster', has_db=False),
    FileTypeInfo(code='img', description='ERDAS Raster', extensions=['.img'], category='raster', has_db=False),
    FileTypeInfo(code='json', description='JSON', extensions=['.json'], category='table', has_db=False),
    FileTypeInfo(code='geojson', description='GeoJSON', extensions=['.geojson'], category='vector', has_db=False),
    FileTypeInfo(code='xlsx', description='Excel', extensions=['.xlsx'], category='table', has_db=False),
    FileTypeInfo(code='dbf', description='DBF', extensions=['.dbf'], category='table', has_db=False),
    # Add more file types as needed
]

@dataclass
class gFileType:
    fcode: str = None
    size_mb: float = None
    has_db: bool = False
    fcat: str = None  # Category of file type
    extension: str = None
    description: str = None

    def __post_init__(self):

        if self.fcode:
            filetype_info = self.get_filetype_info_by_code(self.fcode)
            if filetype_info:
                self.extension = filetype_info.extensions[0]
                self.description = filetype_info.description
                self.fcat = filetype_info.category
                self.has_db = filetype_info.has_db
            else:
                raise ValueError(f"Unknown filetype code: {self.fcode}")
        elif self.extension:
            filetype_info = self.get_filetype_info_by_extension(self.extension)
            if filetype_info:
                self.fcode = filetype_info.code
                self.description = filetype_info.description
                self.fcat = filetype_info.category
                self.has_db = filetype_info.has_db
            else:
                raise ValueError(f"Unknown extension: {self.extension}")
        else:
            raise ValueError("Either filetype_code or extension must be provided.")

    @staticmethod
    def get_filetype_info_by_code(code):
        for ft in FILE_TYPES:
            if ft.code == code:
                return ft
        return None

    @staticmethod
    def get_filetype_info_by_extension(extension):
        for ft in FILE_TYPES:
            flogger.debug(f"Checking extension: {extension} against {ft.extensions}")
            if extension.lower() in [ext.lower() for ext in ft.extensions]:
                return ft
        return None

    @classmethod
    def from_path(cls, path, exist_check=False):
        """
        Create a FileType object from a file path.
        File path need not exist unless exist_check is True. Default: False.
        """
        flogger.info(f"Getting file type from path: {path}")
        if not path:
            raise ValueError("Path must be provided.")
        if exist_check and not os.path.exists(path):
            raise FileNotFoundError(f"The path '{path}' does not exist.")
        elif os.path.exists(path):
            size_bytes = os.path.getsize(path) if os.path.isfile(path) else cls.get_directory_size_bytes(path)
            size_mb = size_bytes / (1024 * 1024)
        else:
            size_mb = None
        flogger.info(f"Size of file: {size_mb} MB")
        extensions = cls.get_extensions(path)
        flogger.info(f"Extensions found: {extensions}")
        if len(extensions) > 1:
            streamlog.warning(f"Multiple extensions found for path '{path}'. Using the first one: {extensions[0]}")
        for ext in extensions:
            filetype_info = cls.get_filetype_info_by_extension(ext)
            flogger.info(f"Filetype info found: {filetype_info}")
            if filetype_info:
                return cls(fcode=filetype_info.code, size_mb=size_mb, extension=ext)
        # If no matching filetype found
        return cls(fcode='UNK', size_mb=size_mb, extension=extensions[-1] if extensions else 'UNK')

    @staticmethod
    def get_extensions(path):
        if "." not in path:
            return []

        base_path, base_name = os.path.split(path)
        if os.path.isdir(path):
            return []

        elif "." not in base_name:
            to_split = os.path.split(base_path)[1] if base_path else path
        else:
            to_split = base_name
        parts = to_split.split('.')
        if len(parts) <= 1:
            return []
        extensions = ['.' + '.'.join(parts[i:]) for i in range(1, len(parts))]
        return extensions

    @staticmethod
    def get_directory_size_bytes(directory):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.exists(fp):
                    total_size += os.path.getsize(fp)
        return total_size

    def update(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"'gFileType' object has no attribute '{key}'")

    def __str__(self):
        attrs = self.__dir__()
        values = [getattr(self, attr) for attr in attrs]
        return ', '.join(f"{attr}={value}" for attr, value in zip(attrs, values))

    def __repr__(self):

        attrs = self.__dir__()
        values = [getattr(self, attr) if not callable(getattr(self, attr))
                                         and getattr(self, attr) is not None else "UNK" for attr in attrs]

        if 'has_db' in attrs:
            values[attrs.index('has_db')] = str(bool(getattr(self, 'has_db')))

        repr_str = (f"{self.__class__.__name__:^12}\n\t\t" + '\t\t'
                    .join(f"{attr:>12}: {value:>12}\n" for attr, value in zip(attrs, values)))
        return repr_str

    def as_dict(self):
        ordered_keys = self.__dir__()
        return {key: getattr(self, key) for key in ordered_keys}

    def __dir__(self):
        return [str(k) for k in self.__dict__.keys()]


####################################################################################################
def isprop(v):
    """Return Bool of whether a dir is a class property"""
    return isinstance(v, property)


def all_properties() -> list:
    """Gets all the tagged class properties for this module's class"""
    return [p for p in dir(ExtLib) if isprop(p)]

####################################################################################################

class ExtLib:

    def __init__(self):
        self.ext_ref = None

    @staticmethod
    def _add_str_cases():
        # print(f'Vars: {[i for i in list(__class__.__dict__.keys()) if not callable(i)]}')
        for ext, reflist in EXT_LOOKUP.items():
            new_list = []
            for ref in reflist:
                new_list.extend([ref, ref.upper(), ref.lower()])
            EXT_LOOKUP[ext] = new_list

    def get_extref(self, str_ref):
        # Add upper and lowercase to reflists
        self.ext_ref = str_ref
        self._add_str_cases()

        # Compare input reference string to
        # Each ref string in each extension dictionary
        returned_ext = None
        for ext, refs in EXT_LOOKUP.items():
            if self.ext_ref in refs:
                ref_index = refs.index(self.ext_ref)
                if f".{refs[ref_index]}" == self.ext_ref:
                    returned_ext = f".{refs[ref_index]}"
                    break
        if returned_ext:
            return returned_ext
        else:
            raise KeyError(f'{self.ext_ref} not found')

# E:\automation\toolboxes\gabelscience\test_data\downloads\nfhl\IA\28_gpkg\NFHL_IA_20241107.gpkg\main.S_FLD_HAZ_AR
def get_extension(ext_ref: [str, None]) -> str:
    if os.path.exists(ext_ref):
        if ".gdb" not in ext_ref:
            file = os.path.split(ext_ref)[1]
            # print(f'File: {file}')
            if "." in file:
                ext_ref = file.split(".")[1]
                howmany_p = len([p for p in file if p == "."])
                if howmany_p > 1:
                    ext_ref = file.split(".")[howmany_p]
        else:
            ext_ref = "gdb"
            # print(f"Extension ref: {ext_ref}")
    elif ".gdb" in ext_ref:
        ext_ref = "gdb"

    extension = ExtLib().get_extref(ext_ref)
    # print(f'  ExtLib Class found {extension}'))
    return extension # TTODO: Deprocate this function


def get_extensions(path) -> List[str]:
    """
    Extracts the extension(s) from a file path, handling multiple periods.

    Parameters:
    - path (str): The file path.

    Returns:
    - extensions (list of str): A list of extensions, each starting with a dot.
    """
    import os

    # Normalize the path
    path = os.path.normpath(path)

    # Get the base name
    base_name = os.path.basename(path)

    # Split the base name on periods
    parts = base_name.split('.')

    # If the first character is a dot (hidden file in Unix), include it
    if base_name.startswith('.'):
        parts = parts[1:]  # Exclude the empty string before the first dot

    # Handle cases where there is no extension
    if len(parts) == 1:
        return []

    # Reconstruct the extensions
    extensions = ['.' + '.'.join(parts[i:]) for i in range(1, len(parts))]

    # For directories like '.gdb', ensure we capture the directory extension
    if os.path.isdir(path):
        dir_ext = os.path.splitext(path)[1].lower()
        if dir_ext and dir_ext not in extensions:
            extensions.append(dir_ext)

    # Return the extensions in order of appearance
    return extensions


def get_spatial_type(file_ext: str) -> str:
    spatialtype = None
    for stype, exts in SPATIAL_TYPES_LOOKUP.items():
        if file_ext in exts:
            spatialtype = stype
            break
    if spatialtype is None:
        raise KeyError(f'{file_ext} not found in spatial files lookup')
    return spatialtype


if __name__ == "__main__":
    # extref = "Shapefile"
    # fext = get_extension(extref)
    # spatial_type = get_spatial_type(fext)
    # print(fext)

    tpath = r"E:\Iowa_00_Tracking\01_statewide\IA_Statewide_Meta\GDB_statewide.gdb\S_Submittal_Statewide_BLE"
    file_type = gFileType.from_path(tpath)
    print(f"File Type:{file_type.__repr__()}")

