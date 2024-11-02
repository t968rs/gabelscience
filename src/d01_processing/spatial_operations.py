import geopandas as gpd
import pandas as pd
import os
import numpy as np
from src.d00_utils.bounds_convert import bbox_to_gdf
from pyproj import Transformer, CRS
from pyproj.aoi import AreaOfInterest, AreaOfUse
from statistics import mean
from tqdm import tqdm
from decimal import Decimal, getcontext, setcontext
from pprint import pprint



##
def convert_linear_units(gdf, column=None, target_crs=None, target_units=None):
    """
    Convert linear units in a GeoDataFrame column to a target unit
    Args:
        gdf: GeoDataFrame
        column: str
        target_crs: CRS
        target_units: str

    Returns:

    """
    # Handle input CRS type
    if not isinstance(target_crs, CRS):
        target_crs = CRS.from_user_input(target_crs)

    # Handle target unit inputs
    if not target_units:
        if not target_crs:
            target_units = "meter"
        else:
            target_units = target_crs.axis_info[0].unit_name
    print(f"Target Units: {target_units}")

    utm_zone = gdf.estimate_utm_crs()
    print(f"UTM Zone: {utm_zone}")
    utm_bounds = utm_zone.area_of_use.bounds
    print(f"UTM Bounds: {utm_bounds}")
    utm_gdf = bbox_to_gdf(bbox_tuple=utm_bounds, crs=4326)
    utm_gdf.to_file(f"./proc_results/utm_bounds_{utm_zone.to_epsg()}.shp")

    # Get AOI and transformer + corner points
    bbox = gdf.total_bounds.tolist()
    bounds_gdf = bbox_to_gdf(bbox, crs=gdf.crs)
    east_lon_degree, north_lat_degree, south_lat_degree, west_lon_degree = bbox
    aoi = AreaOfInterest(east_lon_degree, north_lat_degree, south_lat_degree, west_lon_degree)

    transformer = Transformer.from_crs(gdf.crs, utm_zone,
                                       always_xy=True, area_of_interest=aoi,
                                       authority="EPSG", allow_ballpark=False)

    # Get distances statistics and create range
    dist_range = np.linspace(gdf[column].min(), gdf[column].max(), num=4)
    print(f"Dist Range: {dist_range}")
    distance_multipliers = []
    dec_context = getcontext()
    dec_context.prec = max(len(str(d)) for d in dist_range)

    # Get the distance multipliers
    bounds_points = bounds_gdf.geometry.exterior.get_coordinates()
    print(f"Bounds Points: {bounds_points}")
    per_meter = ""
    for i, row in bounds_points.iterrows():

        # Get the exterior coordinates
        x, y = row.x, row.y
        print(f" X: {x} \n Y: {y} \n")
        for d in dist_range:
            x, y, dist = transformer.transform(x, y, d)
            d = Decimal(d)  # Keep precision
            if target_units == "meter":
                dec_context.prec = 4
                per_meter = '1.000'
            elif target_units == "feet":
                per_meter = '3.28084'
            elif target_units == "US_Foot":
                per_meter = '3.28083333'
            else:
                raise ValueError(f"Invalid target unit: {target_units}")

            # Do the math with precision
            dec_context.prec = len(per_meter.split(".")[1])
            per_meter = Decimal(per_meter)
            dist = Decimal(dist)
            multiplier = d / dist * per_meter if per_meter else d / dist
            distance_multipliers.append(multiplier)
    # print(f"Distance Multipliers: {distance_multipliers}")
    return mean(distance_multipliers)


def export_every_nth_to_new_list(input_list, n):
    """
    Export every nth item from a list to a new list
    :param input_list: list
    :param n: int
    :return: list
    """
    new_list = []
    for i in range(0, len(input_list), n):
        new_list.append(input_list[i])
    return new_list


def process_near_table(gdf, tolerance=10, unique_id_column=None, target_crs=None):
    """
    Process a near table in a GeoDataFrame
    Args:
        gdf:
        tolerance:
        unique_id_column:
        target_crs:

    Returns:

    """
    if isinstance(target_crs, int):
        target_crs = CRS.from_epsg(target_crs)

    gdf = gdf.reset_index(drop=True)
    # Handle unique IDs column
    if not unique_id_column:
        unique_id_column = "unique_id"
    if unique_id_column not in gdf.columns:
        gdf[unique_id_column] = range(len(gdf))

    # Replace -1 with None
    for field in ["NEAR_FID", "NEAR_DIST"]:
        if field in gdf.columns:
            gdf[field] = gdf[field].replace(-1, np.nan)

    # Convert degrees to linear units
    # Create a spatial index and projected CRS if necessary
    if not gdf.crs.is_projected:
        dist_multiplier = convert_linear_units(gdf, column="NEAR_DIST", target_crs=target_crs)
        print(f"Distance Multiplier: {dist_multiplier}")
        new_col_name = f"NEAR_DIST_{target_crs.to_epsg()}"
        gdf[new_col_name] = gdf["NEAR_DIST"] * float(dist_multiplier)
        tolerance = dist_multiplier * tolerance  # linear units
        print(f"Tolerance: {tolerance}")
    else:
        gdf["distance"] = gdf["NEAR_DIST"]

    # Filter by within tolerance
    print(f'Distances: {gdf["distance"].unique()}')
    within_tol = gdf.loc[gdf["distance"].notnull()]
    to_delete = []
    if len(within_tol) > 1:
        within_tol = within_tol.reset_index(drop=True)
        within_tol = within_tol.loc[gdf["distance"] <= tolerance]
        to_delete.extend(export_every_nth_to_new_list(
            within_tol[unique_id_column].tolist(), 2))

    return gdf, list(set(to_delete))


if __name__ == "__main__":
    shp = r"E:\nebraska_BLE\03_delivered\Little_Nemaha\DRAFT_DFIRM_LNM_02\S_BFE.shp"
    bfe_gdf = gpd.read_file(shp)
    bfe_gdf, delete_list = process_near_table(bfe_gdf, tolerance=6, unique_id_column="unique_id", target_crs=26852)
    bfe_gdf.to_file("")
    pprint(delete_list, width=80, compact=True)
    print(bfe_gdf["distance"][:5])
    print("Done")
