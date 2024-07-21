import geopandas as gpd
from shapely.geometry import box
from shapely.geometry import Polygon
import rasterio
import rasterio.merge
import rasterio.windows
import rasterio.warp
import os


def bounds_to_polygon(xmin, ymin, xmax, ymax, crs):
    # Create GDF polygon from bounding box
    bbox = box(xmin, ymin, xmax, ymax)

    pg = Polygon(bbox)

    return pg, gpd.GeoDataFrame(index=[0], geometry=[pg], crs=crs)


def create_mosaic_extent(rasterlist):
    leftlist = []
    bottomlist = []
    rightlist = []
    toplist = []
    crs = None
    for path in rasterlist:
        # print(f'Raster Name: {os.path.split(path)[1]}')
        dataset = rasterio.open(path)
        window = rasterio.windows.get_data_window(dataset.read(1))
        if not crs:
            crs = dataset.crs
        bounds = rasterio.windows.bounds(window, dataset.transform)
        leftlist.append(bounds[0])
        bottomlist.append(bounds[1])
        rightlist.append(bounds[2])
        toplist.append(bounds[3])
    max_bounds = (min(leftlist), min(bottomlist), max(rightlist), max(toplist))
    print(f'Looked through {len(rasterlist)} rasters for spatial extents')
    print(f'Max Bounds: {max_bounds}\n')

    # export bounds as SHP
    bbox = box(*max_bounds)
    geom = [*bbox.exterior.coords]
    # print(geom)
    geom = Polygon(geom)
    print(geom)
    gdf = gpd.GeoDataFrame(index=[0], geometry=[geom], crs=crs)
    box_path = os.path.join(r"A:\Iowa_1A\02_mapping\Grids_ApplePlum", "bounds7.shp")
    gdf.to_file(box_path)

    return max_bounds


def bbox_to_gdf(bbox_tuple, crs, name_str=None, outfolder=None):
    # function to return polygon
    # long0, lat0, lat1, long1
    west, south, east, north = bbox_tuple
    vertices = [
        (west, south),
        (east, south),
        (east, north),
        (west, north),
        (west, south)]  # Closing the polygon by repeating the first vertex
    polygon = Polygon(vertices)

    gdf = gpd.GeoDataFrame(geometry=[polygon], crs=crs)

    gdf = gdf.buffer(0)
    gdf = gdf[~gdf.is_empty]  # Step 2: Delete Null Geometry
    gdf = gdf.explode(index_parts=False)
    gdf.reset_index(drop=True, inplace=True)  # Optionally, reset index
    if outfolder is not None and name_str is not None:
        outpath = os.path.join(outfolder, f"box_test_{name_str}.shp")
        gdf.to_file(outpath)
    print(f'\n  Created pg from bounds')

    return gdf
