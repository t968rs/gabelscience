
"""Takes an input raster and ouputs a series of tiles based on the number of tiles specified"""

import math
import os
import rasterio
from shapely import geometry
from rasterio.mask import mask


class Tiler:

    def __init__(self, in_raster, tile_number):

        self.number_tiles = tile_number
        self.input_raster = in_raster

        outfolder = os.path.join(self.path_names[0], self.path_names[1] + "_tiled")
        if not os.path.exists(outfolder):
            os.makedirs(outfolder)
        self.outfolder = outfolder

    @property
    def path_names(self):
        in_folder, inname = os.path.split(self.input_raster)
        inname = inname.split(".")[0]

        return in_folder, inname

    @staticmethod
    def get_tile_geom(raster, x, y, square):

        transform = rasterio.open(raster).transform
        corner1 = transform * (x, y)
        corner2 = transform * (x + square, y + square)

        return geometry.box(corner1[0], corner1[1],
                            corner2[0], corner2[1])

    @staticmethod
    def get_props(raster, tiles):
        ds = rasterio.open(raster)
        high = ds.height
        wide = ds.width
        insquare_side = int(math.sqrt(high * wide))
        tile_side = int(round(insquare_side / tiles))
        square_tile = tile_side ** 2

        tile_wide = wide // square_tile
        tile_tall = high // square_tile
        print(f'Input cells: {high}, {wide}')
        print(f'Output cell size: {tile_side}')

        return high, wide, square_tile

    # Crop the dataset using the generated box and write it out as a GeoTIFF
    def get_tile_from_raster(self, geom, count):
        ds = rasterio.open(self.input_raster)
        # ds = rioxarray.open_rasterio(raster, chunks=True, lock=False)
        crop, crop_transform = mask(ds, [geom], crop=True)
        outpath = os.path.join(str(self.outfolder), str(self.path_names[1]) + f"_{str(count)}")
        metadata = ds.meta
        incrs = ds.crs

        print(f' Writing {count + 1} / {self.number_tiles} tiles')
        self.write_tiff(crop,
                        crop_transform,
                        metadata,
                        incrs,
                        outpath)

    # Write the passed in dataset as a GeoTIFF
    @staticmethod
    def write_tiff(raster, transform, metadata, crs, outpath):
        metadata.update({"driver": "GTiff",
                         "height": raster.shape[1],
                         "width": raster.shape[2],
                         "transform": transform,
                         "crs": crs})
        with rasterio.open(outpath + ".tif", "w", **metadata) as dest:
            dest.write(raster)

    def split_raster_tiles(self):
        high, wide, square_tile = self.get_props(raster=self.input_raster, tiles=self.number_tiles)
        x, y = 0, 0
        count = 0
        for hc in range(high):
            print(f'Working out tile: {count + 1}...')
            y = hc * square_tile
            for wc in range(wide):
                x = wc * square_tile
                geom = self.get_tile_geom(self.input_raster, x, y, square_tile)
                print(f' Found tile geo')
                self.get_tile_from_raster(geom, count)
                count += 1


if __name__ == "__main__":
    input_raster = r"A:\Grids_LRR\02_WSE_masked\WSE_0_2pct.tif"
    number_tiles = 25

    initialize = Tiler(input_raster, number_tiles)
    initialize.split_raster_tiles()
