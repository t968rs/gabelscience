import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely.geometry import Polygon
import rasterio
import rasterio.merge
import rasterio.windows
import rasterio.warp
import os
from src.d00_utils.open_spatial import open_fc_any


def string_to_affine(s):
    from rasterio.transform import Affine
    parts = str(s).split(',')  # Assuming the string is comma-separated
    print(f'Parts: {parts}')
    if len(parts) != 6 or len(parts) != 9:
        return False, "String does not contain exactly six parts"

    try:
        coefficients = [float(part) for part in parts]
    except ValueError:
        return False, "One or more parts of the string could not be converted to float"

    return True, Affine(*coefficients)


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
        # noinspection PyTypeChecker
        bounds: tuple = rasterio.windows.bounds(window, dataset.transform)
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


def bbox_to_gdf(bbox_tuple, crs, name_str=None, outfolder=None) -> gpd.GeoDataFrame:
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

    data = {"crs": crs}
    gdf = gpd.GeoDataFrame([data], geometry=[polygon], crs=crs)

    gdf.geometry = gdf.geometry.buffer(0)
    gdf = gdf[~gdf.is_empty]  # Step 2: Delete Null Geometry
    gdf = gdf.explode(index_parts=False)
    gdf.reset_index(drop=True, inplace=True)  # Optionally, reset index
    if outfolder is not None and name_str is not None:
        outpath = os.path.join(outfolder, f"box_test_{name_str}.shp")
        gdf.to_file(outpath)
        print(f"  Created: {outpath}")
    print(f'\n  Created pg from bounds\n')

    return gdf


def extract_row_bounds(row, field_name):
    return row[field_name], row.geometry.bounds


def fc_extents_to_dict(fc, field_name) -> dict:
    # Check if field_name exists in the GeoDataFrame
    gdf = open_fc_any(fc)
    epsg_list = [4269]
    if "EPSG_Code" in gdf.columns:
        for prj_code in gdf["EPSG_Code"].unique():
            epsg_list.append(prj_code)
    if field_name not in gdf.columns:
        print(f'Columns: {gdf.columns}')
        raise KeyError(f"Field name '{field_name}' not found in the GeoDataFrame columns.")

    for epsg in epsg_list:
        print(f'EPSG: {epsg}, {type(epsg)}')
        if epsg != 4269:
            gdf_prj = gdf.loc[gdf["EPSG_Code"] == epsg]
            print(f'  GDF PRJ: {gdf_prj.head()}')
            print(f'  GDF PRJ Count: {len(gdf_prj)}')
            gdf_prj = gdf_prj.to_crs(epsg=epsg)
            extents = dict(gdf_prj.apply(lambda row: extract_row_bounds(row, field_name), axis=1))
            yield extents, epsg
        else:
            extents = dict(gdf.apply(lambda row: extract_row_bounds(row, field_name), axis=1))
            yield extents, epsg


def dict_to_df(dictionary, index_field) -> pd.DataFrame:
    # Convert dictionary to DataFrame
    df = pd.DataFrame.from_dict(dictionary, orient='index', columns=[index_field, "Bounds"])
    # print(f'DF: {df}')
    return df


def extent_excel_from_fc(fc, fieldname, out_folder):
    outpaths = []
    # Get extents from feature class
    for extents, epsg in fc_extents_to_dict(fc, fieldname):
        # Convert dictionary to DataFrame
        print(f'EPSG: {epsg} has {len(extents)} extents')
        print(f'  Extents: {extents}')
        df = dict_to_df(extents, index_field=fieldname)
        df["westbc"] = df["Bounds"].apply(lambda x: x[0])
        df["southbc"] = df["Bounds"].apply(lambda x: x[1])
        df["eastbc"] = df["Bounds"].apply(lambda x: x[2])
        df["northbc"] = df["Bounds"].apply(lambda x: x[3])
        df["crs"] = epsg

        # Write to Excel
        os.makedirs(out_folder, exist_ok=True)
        outpath = os.path.join(out_folder, f"{fieldname}_Extents_{epsg}.xlsx")
        print(f'Outpath: {outpath}')
        if os.path.exists(outpath):
            os.remove(outpath)
        if epsg == 4269:
            df.to_excel(outpath, sheet_name=f"{fieldname}_Extents_DRAFT", index=False)
        else:
            df.to_excel(outpath, sheet_name=f"{fieldname}_Extents_{epsg}", index=False)
        outpaths.append(outpath)
    return outpaths


if __name__ == "__main__":
    feature_class = r"E:\nebraska_BLE\02_mapping\NE_Overall_Mapping.gdb\S_Submittal_HUC8_NE"
    field_name = "HUC8"
    out_loc = r"E:\CTP_Metadata\NE"

    outpaths = extent_excel_from_fc(feature_class, field_name, out_loc)
    print(f"Finished")
    for path in outpaths:
        print(f"Saved: {path}")
