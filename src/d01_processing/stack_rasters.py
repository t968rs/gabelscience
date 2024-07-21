import xml.etree.ElementTree as ET

import rasterio
from rasterio.shutil import copy as riocopy
from rasterio.io import MemoryFile
from src.d00_utils import RasterFinder


class StackTIFFasVRT:
    @staticmethod
    def stack_vrts(srcs, band=1):
        vrt_bands = []
        for srcnum, src in enumerate(srcs, start=1):
            with rasterio.open(src) as ras, MemoryFile() as mem:
                riocopy(ras, mem.name, driver='VRT')
                vrt_xml = mem.read().decode('utf-8')
                vrt_dataset = ET.fromstring(vrt_xml)
                for bandnum, vrt_band in enumerate(vrt_dataset.iter('VRTRasterBand'), start=1):
                    if bandnum == band:
                        vrt_band.set('band', str(srcnum))
                        vrt_bands.append(vrt_band)
                        vrt_dataset.remove(vrt_band)
        for vrt_band in vrt_bands:
            vrt_dataset.append(vrt_band)

        return ET.tostring(vrt_dataset).decode('UTF-8')

    def create_vrt_from_folder(self, folder):
        raster_paths = RasterFinder().get_raster_list(folder)
        print(f"Rasters: {raster_paths}")

        with rasterio.open(self.stack_vrts(raster_paths)) as src:
            return src, src.profile


