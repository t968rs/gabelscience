import os
import geopandas as gpd
import pandas as pd
import numpy as np
import typing as T
from src.d00_utils.open_spatial import open_fc_any
from src.d00_utils.unique_id import create_all_unique_id


def multi_buffer(input_gdf, buff_dist, buffer_number):
    """
    Buffer a GeoDataFrame by multiple distances
    Args:
        buffer_number:
        buff_dist:
        input_gdf: GeoDataFrame
        buffer_dict: dict

    Returns:

    """

    # Create buffer dictionary
    buffer_lookup = create_buffer_dict_from_tolerance(buff_dist, buffer_number)
    print(f"Buffer Lookup: {buffer_lookup}")

    # Buffer input_gdf
    out_buffs = []
    for k, buff_dist in buffer_lookup.items():
        print(f"Buffering {k} by {buff_dist}")
        buff_geo = input_gdf.geometry.buffer(buff_dist, resolution=4)
        print(f" Buff objects: {len(buff_geo)}")
        for geom in buff_geo:
            out_buffs.append({"geometry": geom, "buff_dist": buff_dist})

    # Convert to GeoDataFrame
    out_buffs = pd.DataFrame(out_buffs)
    input_gdf["buff_dist"] = 0
    out_buffs = pd.concat([input_gdf, out_buffs], ignore_index=True)
    out_buffs = gpd.GeoDataFrame(out_buffs, crs=input_gdf.crs, geometry='geometry')
    return remove_empty_geometries(out_buffs)


def polygon_to_line_gdf(input_gdf, keeper_columns=None):
    """
    Convert a polygon GeoDataFrame to a line GeoDataFrame
    Args:
        keeper_columns: T.Union[None, T.List[str], T.Str]
        input_gdf: GeoDataFrame

    Returns:

    """
    input_gdf = remove_empty_geometries(input_gdf)

    # Handle keeper_columns input
    if isinstance(keeper_columns, str):
        keeper_columns = [keeper_columns]

    # Get tabular data from input_gdf
    lines_data = {}
    for keeper in keeper_columns:
        if keeper in input_gdf.columns:
            lines_data[keeper] = input_gdf[keeper]

    # Get exterior boundary
    lines = input_gdf.geometry.boundary
    lines_data["geometry"] = lines
    lines_gdf = gpd.GeoDataFrame(lines_data, crs=input_gdf.crs, geometry='geometry')
    return lines_gdf


def remove_empty_geometries(input_spatial: T.Union[gpd.GeoDataFrame, gpd.GeoSeries]):
    """
    Remove empty geometries from a GeoDataFrame
    Args:
        input_spatial: GeoDataFrame or GeoSeries

    Returns: GeoDataFrame or Geoseries

    """
    # Remove empty geometries
    return input_spatial.loc[input_spatial.geometry.notna() & ~input_spatial.geometry.is_empty]


def create_buffer_dict_from_tolerance(tol, buff_no):
    """
    Create a buffer dictionary from a tolerance and number of buffers
    Args:
        tol: float
        buff_no: int

    Returns:

    """
    buffers = {}
    stop_buff = tol * buff_no
    for i, buff_amount in enumerate(np.linspace(tol, stop_buff, num=buff_no)):
        n = i + 1
        buffers[n] = round(buff_amount, 1)
    return buffers


def create_random_sample_points(input_gdf: gpd.GeoDataFrame,
                                crs_units_per_sample: int):
    """
    Create a random sample of points from a GeoDataFrame
    Args:
        crs_units_per_sample: int
        input_gdf: GeoDataFrame

    Returns:

    """
    input_gdf['sample_size'] = input_gdf.geometry.length / crs_units_per_sample
    input_gdf['sample_size'] = input_gdf['sample_size'].apply(np.rint)
    input_gdf['sample_size'] = input_gdf['sample_size'].replace(0, 1)
    input_gdf['sample_size'] = input_gdf['sample_size'].astype(int)
    unique_id = input_gdf["unique_id"]
    print(f"Sample Size: {input_gdf['sample_size']}")

    sample_points = []
    for idx, row in input_gdf.iterrows():
        points = [row.geometry.interpolate(np.random.random(), normalized=True) for _ in range(row['sample_size'])]
        sample_points.extend(points)

    return gpd.GeoDataFrame({"geometry": sample_points, "unique_id": unique_id.repeat(input_gdf['sample_size'])},
                            crs=input_gdf.crs, geometry='geometry')


if __name__ == "__main__":
    input_shp = r"E:\Iowa_1A\02_mapping\Review_Copperas\cup_testing\Zone_A_3418.shp"
    number_buffers = 3
    buff_dist = -3.3
    ingdf = open_fc_any(input_shp)
    create_all_unique_id(ingdf, 100, 5, "unique_id")
    workingfolder = os.path.split(input_shp)[0]
    multi_buffed = multi_buffer(ingdf, buff_dist, number_buffers)
    buff_amounts = multi_buffed["buff_dist"].unique().tolist()
    print(f"Buffer Amounts: {buff_amounts}")
    for i, buff in enumerate(buff_amounts):
        buffed = multi_buffed.loc[multi_buffed["buff_dist"] == buff]
        buffed.to_file(os.path.join(workingfolder, f"Zone_A_{i + 1}.shp"))
        print(f"Buffered {i + 1} by {buff}")

        bufflines = polygon_to_line_gdf(buffed, keeper_columns=["buff_dist", "unique_id"])
        samples = create_random_sample_points(bufflines, 10)
        samples.to_file(os.path.join(workingfolder, f"Zone_A_{i + 1}_samples.shp"))
