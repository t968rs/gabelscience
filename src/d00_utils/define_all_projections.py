import os

import rasterio


class DefinIowaProjections:

    def __init__(self, input_folder, unk_crs):

        self.input_folder = input_folder
        self.unk_crs = unk_crs
        self.raster_paths = None
        self._init_raster_list()

    @property
    def bad_exts(self):
        bad_exts = {".img": [".rrd", ".aux", ".xml"],
                    ".tif": [".xml", ".ovr"]}
        return bad_exts

    @property
    def raster_exts(self):
        return [".img", ".tif"]

    def _init_raster_list(self):

        raster_paths = []
        print(f'Looking in folder, {self.input_folder}')

        score_keeper = {}
        for root, folders, files in os.walk(self.input_folder):
            # print(f'Root: {root}\n{folders}\n {files}')
            for file in files:
                # print(f'File: {file}')
                path = os.path.join(root, os.path.join(root, file))
                score_keeper[path] = [0, 0]
                for ext in self.raster_exts:
                    if ext in path.lower() and "autolock" not in path.lower():
                        score_keeper[path][0] += 1
                        for _ in self.bad_exts[ext]:
                            if _ in path.lower():
                                score_keeper[path][1] += 1
        for path, scores_tuple in score_keeper.items():
            print(f'Path: {path}\n  {scores_tuple}')
            if scores_tuple[0] >= 1:
                if scores_tuple[1] < 1:
                    print(f' Good Path: {path}')
                    raster_paths.append(path)
            else:
                print(f" Bad score: {path}")

        self.raster_paths = raster_paths

    @staticmethod
    def check_crs(path):

        ds = rasterio.open(path)
        checkcrs = ds.crs

        print(f'EPSG: {checkcrs.to_epsg()}')
        if checkcrs.to_epsg() is None:
            print(f'CRS WKT: {checkcrs.to_wkt()}')

    def define_projections(self):

        for path in self.raster_paths:
            print(f'Raster: {os.path.split(path)[1]}')
            dataset = rasterio.open(path, "r+")
            incrs = dataset.crs
            if incrs is not None:
                if "Iowa North" in incrs.to_wkt() or "Iowa_North" in incrs.to_wkt():
                    outcrs = 3417
                elif "Iowa South" in incrs.to_wkt() or "Iowa_South" in incrs.to_wkt():
                    outcrs = 3418
                else:
                    outcrs = self.unk_crs
            else:
                outcrs = self.unk_crs

            if outcrs is not None:
                dataset.crs = rasterio.crs.CRS.from_epsg(outcrs)
                print(f' Defined with: {outcrs}')

        print(f'Looked through {len(self.raster_paths)} for CRS EPSG\n\n')
        for path in self.raster_paths:
            self.check_crs(path)


if __name__ == "__main__":
    infolder = r"A:\nebraska_BLE\02_mapping\keg\testing"
    other_crs = 26852

    initialize = DefinIowaProjections(infolder, other_crs)
    initialize.define_projections()
