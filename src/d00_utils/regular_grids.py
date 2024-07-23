"""Creates regular grids for a given geodataframe or bounds tuple"""

import geopandas as gpd
import shapely
import numpy as np
import os
import typing as T


def get_width_height(bounds):
    """
    Calculate the width and height from a bounds tuple.

    Parameters:
    - bounds (tuple): A tuple of the form (xmin, ymin, xmax, ymax)

    Returns:
    - width (float): The width of the bounding box.
    - height (float): The height of the bounding box.
    """
    xmin, ymin, xmax, ymax = bounds
    width = xmax - xmin
    height = ymax - ymin
    return width, height


def create_regular_grid(gdf: gpd.GeoDataFrame = None,
                        bounds: tuple = None,
                        n_cells: tuple = (2, 2),
                        overlap: bool = False,
                        epsg_code: int = None) -> T.Tuple[gpd.GeoDataFrame, int, int]:
    """Create square grid that covers a geodataframe area
    """
    assert bounds is not None or gdf is not None, "Either gdf or bounds must be provided"
    if gdf is None:
        xmin, ymin, xmax, ymax = bounds
    else:
        xmin, ymin, xmax, ymax = gdf.total_bounds

    # get cell size
    if isinstance(n_cells, int):
        cell_size = (xmax - xmin) / n_cells
        cell_sizex, cell_sizey = cell_size, cell_size
    else:
        cell_sizex = (xmax - xmin) / n_cells[0]
        cell_sizey = (ymax - ymin) / n_cells[1]
    print(f'    Tile sizes: {round(cell_sizex)}, {round(cell_sizey)}')
    # create the cells in a loop
    grid_cells = []
    for x0 in np.arange(xmin, xmax, cell_sizex):
        for y0 in np.arange(ymin, ymax, cell_sizey):
            x1 = x0 + cell_sizex
            y1 = y0 + cell_sizey
            poly = shapely.geometry.box(x0, y0, x1, y1)
            # print (gdf.overlay(poly, how='intersection'))
            grid_cells.append(poly)

    # Create GeoDataFrame with only the 'geometry' column
    cells_gdf = gpd.GeoDataFrame(grid_cells, columns=['geometry'], crs=f"EPSG:{epsg_code}")
    cells_gdf.set_crs(epsg=epsg_code, inplace=True)

    # If overlap is True, perform spatial join and filter out non-overlapping cells
    if overlap and gdf is not None:
        cells_gdf = (gpd.sjoin(cells_gdf, gdf, how='inner', predicate='intersects').drop_duplicates('geometry')
                     .reset_index(drop=True))

    # Add grid_id and grid_area columns
    cells_gdf['grid_id'] = range(len(cells_gdf))
    cells_gdf['grid_area'] = round(cells_gdf['geometry'].area, 2).astype("float32")

    return cells_gdf, n_cells, len(grid_cells)


def get_gdf_from_fc(path):
    # Read Polygon Shapefile
    if ".gdb" in path:
        gdb, fc = os.path.split(path)
        # print(fiona.listlayers(gdb))
        gdf = gpd.read_file(gdb, driver='FileGDB', layer=fc)
    else:
        gdf = gpd.read_file(path)
    c_list = [c for c in gdf.columns.to_list()]
    return gdf, c_list


if __name__ == "__main__":
    shppath = r"A:\Iowa_3B\02_mapping\Grids_Rock\zz_Vectors\examples.shp"
    ingdf = get_gdf_from_fc(shppath)[0].to_crs(epsg=3417)
    grid = create_regular_grid(ingdf, n_cells=(5, 5), epsg_code=3417)[0].loc[:, ["geometry"]]
    grid.to_file(r"A:\Iowa_3B\02_mapping\Grids_Rock\zz_Vectors\grid.shp", driver="ESRI Shapefile")