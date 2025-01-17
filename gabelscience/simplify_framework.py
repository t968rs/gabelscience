import os
import math
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon, LineString
from shapely.ops import polygonize, unary_union

from multiprocessing import Pool, cpu_count  # or use concurrent.futures, or dask


def separate_isolated_polygons(gdf: gpd.GeoDataFrame) -> (gpd.GeoDataFrame, gpd.GeoDataFrame):
    """
    Splits gdf into two groups:
      1) 'isolated_gdf': polygons that do NOT intersect/touch any other polygon
      2) 'adjacent_gdf': polygons that do intersect/touch another polygon
    """
    # For adjacency, we can do a spatial index approach or a 'buffer(0)' approach
    # but simplest (though not always the fastest) is a quick "touches/intersects" check.

    # A naive approach:
    #   - If 'count of intersects with others' == 1, it's isolated. Else it's adjacent.
    #   (Because the polygon always intersects itself, so we expect count >= 1)

    # We can do an sjoin, or a groupby on intersects. Let's try something conceptual:
    sindex = gdf.sindex
    idxs_isolated = []
    idxs_adjacent = []

    for idx, row in gdf.iterrows():
        geom = row.geometry
        possible_matches_index = list(sindex.intersection(geom.bounds))
        # Filter out self
        possible_matches_index = [i for i in possible_matches_index if i != idx]

        # Count how many geometries actually intersect or touch
        intersects_count = 0
        for i in possible_matches_index:
            if geom.intersects(gdf.at[i, 'geometry']) or geom.touches(gdf.at[i, 'geometry']):
                intersects_count += 1

        if intersects_count == 0:
            # This means no adjacency with others
            idxs_isolated.append(idx)
        else:
            idxs_adjacent.append(idx)

    isolated_gdf = gdf.loc[idxs_isolated].copy()
    adjacent_gdf = gdf.loc[idxs_adjacent].copy()

    return isolated_gdf, adjacent_gdf


def simplify_polygons_in_parallel(gdf: gpd.GeoDataFrame, func, n_jobs: int = None) -> gpd.GeoDataFrame:
    """
    Given a GeoDataFrame and a 'func' that simplifies polygons (or lines, etc.),
    run it in parallel. 'func' should take (geometry) and return simplified geometry.
    """

    # We'll map geometry -> geometry in parallel.
    # This is a naive approach using multiprocessing Pool.
    if n_jobs is None:
        n_jobs = max(1, cpu_count() - 1)

    geoms = gdf['geometry'].tolist()

    with Pool(n_jobs) as pool:
        simplified_geoms = pool.map(func, geoms)

    out_gdf = gdf.copy()
    out_gdf['geometry'] = simplified_geoms
    return out_gdf


def convert_polygons_to_lines_and_dissolve(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Converts polygons to boundary lines, then dissolves so that
    shared boundaries become single line segments.
    """
    # 1) Polygons -> lines
    # Each polygon boundary is a (Multi)LineString.
    # We'll just do geometry.boundary for each row.
    lines_gdf = gdf.copy()
    lines_gdf['geometry'] = lines_gdf['geometry'].boundary

    # 2) Dissolve everything (optional: by some grouping if you want subsets).
    # For example, if you want to dissolve all into one big linestring set:
    dissolved = lines_gdf.dissolve(by=None)  # or by="group_id"
    # Now dissolved is a single row, with a MultiLineString geometry (potentially).

    # Convert dissolved to a new GDF.
    # We'll keep the same CRS, if any.
    dissolved_gdf = gpd.GeoDataFrame(geometry=[dissolved.geometry.iloc[0]], crs=gdf.crs)

    return dissolved_gdf


def lines_to_polygons(lines_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Polygonize the lines back to polygons.
    """
    # Combine all lines into a single geometry, then polygonize
    multi_line = unary_union(lines_gdf.geometry)
    polygons = list(polygonize(multi_line))

    poly_gdf = gpd.GeoDataFrame(geometry=polygons, crs=lines_gdf.crs)
    return poly_gdf


def largest_overlap_join(simplified_polygons: gpd.GeoDataFrame,
                         original_polygons: gpd.GeoDataFrame,
                         attr_cols=None) -> gpd.GeoDataFrame:
    """
    For each polygon in 'simplified_polygons', find the original polygon(s)
    that overlap it. Assign attributes from the 'largest overlap' original polygon.

    Returns a new GDF with columns from simplified_polygons + selected columns from original_polygons.
    """
    if attr_cols is None:
        attr_cols = [c for c in original_polygons.columns if c != 'geometry']

    # Strategy:
    #   1) Compute intersection areas (gpd.overlay or manual approach).
    #   2) For each simplified poly, find which original poly has the max intersection area.
    #   3) Assign that original polygon's attributes.

    # We'll do an overlay in "intersection" mode, get the area, then pick the max area for each simplified poly.
    intersect_gdf = gpd.overlay(simplified_polygons, original_polygons, how='intersection')

    # We'll compute area of the intersection for each row
    intersect_gdf['int_area'] = intersect_gdf.area

    # Suppose the simplified polygons get an index S, the original polygons get an index O.
    # The overlay might give us columns: S: index_simplified, O: index_original, plus geometry.

    # We'll group by the simplified geometry's index and pick the row with largest int_area
    # Then we can merge back.

    # Let's store the index columns for clarity
    # In newer geopandas, the overlay might rename indexes to something like 'index_1' and 'index_2'
    # or keep them as 'level_0', 'level_1'. We'll discover them:
    # For simplicity, let's rename them explicitly:

    intersect_gdf = intersect_gdf.reset_index(drop=True)
    # We'll assume 'index_left' is from simplified_polygons, 'index_right' is from original_polygons
    # If geopandas uses 'index_1', 'index_2', adjust accordingly.

    # A trick: store them before the overlay (the code below is a conceptual example)
    simplified_polygons = simplified_polygons.reset_index(drop=False).rename(columns={'index': 'simp_idx'})
    original_polygons = original_polygons.reset_index(drop=False).rename(columns={'index': 'orig_idx'})

    intersect_gdf = gpd.overlay(simplified_polygons, original_polygons, how='intersection')
    intersect_gdf['int_area'] = intersect_gdf.area

    # Now we do groupby on 'simp_idx'
    # For each group, pick the row with the largest intersection area
    idxmax_series = intersect_gdf.groupby('simp_idx')['int_area'].idxmax()

    # Gather those rows
    best_matches = intersect_gdf.loc[idxmax_series]

    # Now 'best_matches' has geometry from the intersection, plus columns from both simplified + original.
    # We'll build a new GDF that has the *simplified polygon geometry* plus the original attributes.

    # Merge back with the full simplified_polygons to get the original geometry
    # (since best_matches geometry is the intersection geometry).
    # We'll store the original geometry from simplified_polygons in a separate DataFrame:
    simp_geom_df = simplified_polygons[['simp_idx', 'geometry']]

    # We'll rename best_matches columns from original to something or keep them as is
    # We'll just pick the original polygon columns (attr_cols) from best_matches
    merged = pd.merge(simp_geom_df, best_matches[['simp_idx'] + attr_cols], on='simp_idx', how='left')

    # Now merged is a DataFrame, convert to GeoDataFrame
    # The geometry in merged is from 'simp_geom_df' (the simplified polygons).
    out_gdf = gpd.GeoDataFrame(merged, geometry='geometry', crs=simplified_polygons.crs)
    return out_gdf


# --------------------------------------------------------------------
#  A CLASS METHOD THAT WIRES THIS ALL TOGETHER (SKELETON)
# --------------------------------------------------------------------
class PolygonAdjacencySimplifier:
    def __init__(self, input_gdf: gpd.GeoDataFrame):
        self.gdf = input_gdf
        self.crs = input_gdf.crs

    def run_workflow(self, simplify_func, n_jobs: int = None):
        """
        1) Separate isolated from adjacent
        2) Parallel simplify isolated polygons
        3) Convert adjacent polygons to lines, dissolve
        4) Parallel simplify lines
        5) Convert lines back to polygons
        6) Reattach attributes with largest-overlap approach
        """

        # 1) Separate
        isolated_gdf, adjacent_gdf = separate_isolated_polygons(self.gdf)

        # 2) Simplify isolated polygons in parallel
        if not isolated_gdf.empty:
            isolated_simplified = simplify_polygons_in_parallel(isolated_gdf, simplify_func, n_jobs=n_jobs)
        else:
            isolated_simplified = isolated_gdf

        # 3) Convert adjacent polygons to lines, dissolve
        if not adjacent_gdf.empty:
            lines_gdf = convert_polygons_to_lines_and_dissolve(adjacent_gdf)

            # 4) Simplify lines in parallel
            simplified_lines = simplify_polygons_in_parallel(lines_gdf, simplify_func, n_jobs=n_jobs)

            # 5) Convert lines back to polygons
            re_poly_gdf = lines_to_polygons(simplified_lines)

            # 6) Largest overlap join
            re_poly_with_attrs = largest_overlap_join(re_poly_gdf, adjacent_gdf)

        else:
            # No adjacent polygons
            re_poly_with_attrs = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs=self.crs)

        # Combine the two sets: isolated_simplified + re_poly_with_attrs
        final_gdf = pd.concat([isolated_simplified, re_poly_with_attrs], ignore_index=True)
        final_gdf.crs = self.crs
        return final_gdf


# --------------------------------------------------------------------
#  USAGE EXAMPLE (CONCEPTUAL)
# --------------------------------------------------------------------
if __name__ == "__main__":
    # Suppose we have a shapefile with polygons
    input_shp = r"/path/to/polygons.shp"
    gdf = gpd.read_file(input_shp)


    # A placeholder simplify function that just returns the original geometry
    # or uses shapely's .simplify() for demo
    def my_simplify_func(geom):
        # E.g., shapely's built-in DP. In reality, you'd do your custom code
        return geom.simplify(0.01, preserve_topology=True)


    pas = PolygonAdjacencySimplifier(gdf)
    result_gdf = pas.run_workflow(simplify_func=my_simplify_func, n_jobs=4)

    # Save final result
    out_shp = r"/path/to/simplified_polygons.shp"
    result_gdf.to_file(out_shp)
    print("Done.")
