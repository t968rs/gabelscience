from concurrent.futures import ThreadPoolExecutor as TPE, as_completed
import geopandas as gpd
import os


def filter_export_gdf(input_gdf: gpd.GeoDataFrame
                      , idx: int,
                      out_folder: str,):
    # Filter
    input_gdf = input_gdf[[c for c in input_gdf.columns if "shape" not in c.lower()]]

    # Project
    input_gdf = input_gdf.to_crs("EPSG:4326")

    # Split
    # zone_a = input_gdf.loc[input_gdf["FLD_ZONE"] == "A"]
    # zone_x = input_gdf.loc[input_gdf["FLD_ZONE"] == "X"]

    outpath_a = os.path.join(out_folder, f"buildings_{idx}.geojson")
    # outpath_x = os.path.join(out_folder, "BLE_Zone_X", f"BLE_Zone_X_{idx}.geojson")

    # Write the files
    input_gdf.to_file(outpath_a, driver="GeoJSON")

    return outpath_a


#
# grid_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52]
#
#
# futures = []
# with TPE(max_workers=min(os.cpu_count(), len(grid_list))) as executor:
#     for idx, grid in enumerate(grid_list):
#         future = executor.submit(filter_export_gdf,
#                                  input_gdf=gdf),
#                                  idx=idx,
#                                  out_folder=r"Z:\Shell_Rock\03_delivery\02_internal_viewer")
#         futures.append(future)
#
#         for future in as_completed(futures):
#             print(future.result())


