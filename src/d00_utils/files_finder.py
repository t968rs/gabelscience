import os


class RasterFinder:
    def __init__(self):
        self.raster_dict = {}
        self.raster_list = []
        self.folder = None
        print(f'  Raster Finder')

    @property
    def bad_exts(self) -> dict:
        bad_exts = {".img": [".rrd", ".aux", ".xml"],
                    ".tif": [".xml", ".ovr"]}
        return bad_exts

    @property
    def raster_exts(self) -> list:
        return [".img", ".tif"]

    def _get_rasters_from_folder(self):
        for root, folders, files in os.walk(self.folder):
            for file in files:
                path = os.path.join(root, file)
                parts = list(path.lower().split("."))
                period_parts = ["." + str(p) for p in parts]
                if not set(self.raster_exts).isdisjoint(set(period_parts)):
                    file_ext = list(set(self.raster_exts).intersection(set(period_parts)))[0]
                    # print(f'File ext: {file_ext}')
                    no_append = False
                    if not set(period_parts).isdisjoint(self.bad_exts[file_ext]):
                        no_append = True
                    if not no_append:
                        filename = file.split(file_ext)[0]
                        self.raster_dict[filename] = {"ext": file_ext, "path": path}
                        self.raster_list.append(path)
        print(f'\nThere are {len(self.raster_dict)} rasters in {self.folder}\n')

    def _populate_folder(self, folder):
        if not self.folder:
            self.folder = folder
        self._get_rasters_from_folder()

    def get_raster_dictionary(self, folder):
        self._populate_folder(folder)
        self._get_rasters_from_folder()
        return self.raster_dict

    def get_raster_list(self, folder):
        self._populate_folder(folder)
        self._get_rasters_from_folder()
        return self.raster_list






