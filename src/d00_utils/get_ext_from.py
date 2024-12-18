# sits in src/d00_utils/get_ext_from.py
"""Extension library and methods for getting file extensions"""


def isprop(v):
    """Return Bool of whether a dir is a class property"""
    return isinstance(v, property)


def all_properties() -> list:
    """Gets all the tagged class properties for this module's class"""
    return [p for p in dir(ExtLib) if isprop(p)]


extension_lookup = {"shp": ["Shapefile", ".shp", "shp"],
                    "geojson": ["GeoJSON", ".geojson", "geojson"],
                    "gdb": ["FileGDB", ".gdb", "gdb"],
                    "tif": ["tif", "TIFF", ".tif"],
                    "img": ["IMG", ".img", "img"]}

spatial_files_lookup = {"vector": [".shp", ".geojson", ".gdb"],
                        "raster": [".tif", ".img"]}


class ExtLib:

    def __init__(self):
        self.ext_ref = None

    @staticmethod
    def _add_str_cases():
        # print(f'Vars: {[i for i in list(__class__.__dict__.keys()) if not callable(i)]}')
        for ext, reflist in extension_lookup.items():
            new_list = []
            for ref in reflist:
                new_list.extend([ref, ref.upper(), ref.lower()])
            extension_lookup[ext] = new_list

    def get_extref(self, str_ref):
        # Add upper and lowercase to reflists
        self.ext_ref = str_ref
        self._add_str_cases()

        # Compare input reference string to
        # Each ref string in each extension dictionary
        returned_ext = None
        for ext, refs in extension_lookup.items():
            if self.ext_ref in refs:
                returned_ext = f".{ext}"
                break
        if returned_ext:
            return returned_ext
        else:
            raise KeyError(f'{self.ext_ref} not found')


def get_extension(ext_ref: [str, None]) -> str:
    import os
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
    return extension


def get_spatial_type(file_ext: str) -> str:
    spatialtype = None
    for stype, exts in spatial_files_lookup.items():
        if file_ext in exts:
            spatialtype = stype
            break
    return spatialtype


if __name__ == "__main__":
    extref = "Shapefile"
    fext = get_extension(extref)
    spatial_type = get_spatial_type(fext)
    print(fext)
