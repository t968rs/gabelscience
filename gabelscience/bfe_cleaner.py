from tqdm import tqdm
import geopandas as gpd
import pandas as pd
from shapely.ops import split
from shapely.geometry import Polygon, LineString, Point
from src.d00_utils.open_spatial import open_fc_any
import os


def keep_longest_line(gdf: gpd.GeoDataFrame, group_field, unique_field) -> gpd.GeoDataFrame:
    # Step 1: Calculate lengths of each line
    gdf_utm = gdf.to_crs(gdf.estimate_utm_crs("NAD83"))
    gdf_utm['length'] = gdf_utm.geometry.length
    gdf_utm = gdf_utm[gdf_utm['length'] > 0]
    gdf_utm.to_file(r"E:\Iowa_3B\02_mapping\Maple_Mapping\BFE\BFE_02_utm.shp")

    # Step 2: Identify indices of the longest lines
    longest_indices = gdf_utm.groupby(group_field)['length'].idxmax()
    longest_indices = longest_indices.drop_duplicates()
    print(f"Longest indices: \n{longest_indices}\n\tLongest calc count: {len(longest_indices)}")

    # Step 3: Get unique_field values for the longest lines
    gdf_filtered = gdf_utm.loc[longest_indices]
    unique_values = gdf_filtered[unique_field].unique().tolist()
    print(f"\tUnique vals count: {len(unique_values)}")

    # Step 4: Filter the original gdf using these unique values
    gdf_filtered = gdf[gdf[unique_field].isin(unique_values)]
    print(f"Filtered GDF: \n{gdf_filtered.head()}")

    return gdf_filtered

def remove_smallest_intersections(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Remove the smallest intersecting lines from a GeoDataFrame.

    :param gdf: GeoDataFrame containing the features
    :return: GeoDataFrame with the smallest intersecting lines removed
    """
    # Ensure the GeoDataFrame has a projected CRS for accurate length calculations
    if not gdf.crs.is_projected:
        gdf = gdf.to_crs(gdf.estimate_utm_crs())

    # Initialize a list to store indices of features to delete
    to_delete = []

    # Iterate over each feature in the GeoDataFrame
    for idx, feature in gdf.iterrows():
        # Find intersecting features
        lengths = {}
        intersecting = gdf[gdf.geometry.intersects(feature.geometry) & (gdf.index != idx)]

        if len(intersecting) == 0:
            continue

        f_length = feature.geometry.length * 3.28084  # Convert meters to feet
        lengths[idx] = f_length

        # Calculate the length of each intersecting line
        for inter_idx, inter_feature in intersecting.iterrows():

            length_ft = inter_feature.geometry.length * 3.28084  # Convert meters to feet
            # Store the index of the smaller feature
            lengths[inter_idx] = length_ft

        # Find the smallest intersecting feature
        min_idx = min(lengths, key=lengths.get)
        to_delete.append(min_idx)

    # Remove duplicates from the to_delete list
    to_delete = list(set(to_delete))

    # Drop the smallest intersecting features
    gdf = gdf.drop(index=to_delete)

    return gdf


def keep_longest_singlepart(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Step 1: Create a field and assign a static ID to each feature
    print(f"Input GDF: \n{gdf.head()}")
    gdf['mp_id'] = range(len(gdf))
    mp_count = len(gdf)

    # Step 2: Convert multi-part geometries to single-part geometries
    gdf_sp = gdf.explode(ignore_index=True)
    gdf_sp['sp_id'] = range(len(gdf_sp))
    print(f"\tExploded from {mp_count} multi-part, to {len(gdf_sp)} single-part features")

    pruned_gdf = keep_longest_line(gdf_sp, 'mp_id', 'sp_id')
    print(f"Pruned GDF: \n{pruned_gdf.head()}")
    print(f"\tOutput has {len(pruned_gdf)} features")

    return pruned_gdf


if __name__ == "__main__":
    bfe_shapefile = r"E:\Iowa_3B\02_mapping\Little_Sioux\BFE\contour_04.shp"

    # Read the shapefiles
    bfe = open_fc_any(bfe_shapefile)
    bfe_utm = bfe.to_crs(bfe.estimate_utm_crs("NAD83"))
    print(f" BFE CRS: {bfe.crs}")
    print(f" BFE UTM CRS: {bfe_utm.crs}")

    # Remove smallest of intersecting lines
    bfe_utm = remove_smallest_intersections(bfe_utm)
    bfe_out = bfe_utm.to_crs(bfe.crs)

    base, filename = os.path.split(bfe_shapefile)
    name, ext = os.path.splitext(filename)
    outname = f"{name}_no_small_intsct{ext}"
    outpath = os.path.join(base, outname)
    bfe_out.to_file(outpath)

    # # Filter lines by endpoints
    bfe_pruned = keep_longest_singlepart(bfe_out)
    outname = f"{name}_longest{ext}"
    outpath = os.path.join(base, outname)
    bfe_pruned.to_file(outpath)

