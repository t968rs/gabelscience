import os
import geopandas as gpd
import pandas as pd
from pyproj import CRS
from shapely import Polygon
from shapely.geometry import box
from src.d00_utils.open_spatial import open_fc_any


class gBounds:
    def __init__(self, bounds, crs):
        self.bounds = bounds  # (xmin, ymin, xmax, ymax)
        self.crs = CRS.from_user_input(crs)  # Coordinate Reference System as pyproj.CRS object


    def to_polygon(self):
        # Create GDF polygon from bounding box
        bbox = box(*self.bounds)
        return bbox, gpd.GeoDataFrame(index=[0], geometry=[bbox], crs=self.crs)

    def to_gdf(self):
        return gpd.GeoDataFrame(index=[0], geometry=[box(*self.bounds)], crs=self.crs)

    def to_esri_json(self):
        return {
            "xmin": self.bounds[0],
            "ymin": self.bounds[1],
            "xmax": self.bounds[2],
            "ymax": self.bounds[3],
            "spatialReference": {
                "wkid": self.crs.to_epsg()
            }
        }

    def to_wkt(self):
        """Creates a bounding box polygon from coordinates and returns WKT format."""
        wkt = f'SRID={self.crs.to_epsg()};POLYGON(({self.bounds[0]} {self.bounds[1]}, {self.bounds[2]} {self.bounds[1]}, {self.bounds[2]} {self.bounds[3]}, {self.bounds[0]} {self.bounds[3]}, {self.bounds[0]} {self.bounds[1]}))'
        return wkt

    @classmethod
    def from_gdf(cls, gdf):
        bounds = gdf.total_bounds
        return cls(bounds, gdf.crs)

    @classmethod
    def from_polygon(cls, polygon, crs=None):
        if not polygon.has_attribute("crs"):
            crs = crs if crs else 4269
        else:
            crs = polygon.crs
        return cls(polygon.bounds, crs)

    @classmethod
    def from_wkt(cls, wkt):
        # Extract bounds from WKT
        bounds = wkt.split("POLYGON((")[1].split("))")[0].split(", ")
        epsg = int(wkt.split("SRID=")[1].split(";")[0])
        crs = CRS.from_user_input(epsg)
        return cls(bounds, crs)

    @classmethod
    def from_bbox(cls, bbox_tuple, crs):
        return cls(bbox_tuple, crs)

    @staticmethod
    def from_raster_list(raster_list):
        import rasterio
        """Calculate maximum bounds from a list of rasters."""
        leftlist, bottomlist, rightlist, toplist = [], [], [], []
        crs = None
        for path in raster_list:
            dataset = rasterio.open(path)
            if not crs:
                crs = dataset.crs
            bounds = dataset.bounds
            leftlist.append(bounds.left)
            bottomlist.append(bounds.bottom)
            rightlist.append(bounds.right)
            toplist.append(bounds.top)
        max_bounds = (min(leftlist), min(bottomlist), max(rightlist), max(toplist))
        return gBounds(max_bounds, crs)


def to_polygon(xmin, ymin, xmax, ymax, crs):
    # Create GDF polygon from bounding box
    bbox = box(xmin, ymin, xmax, ymax)

    pg = Polygon(bbox)

    return pg, gpd.GeoDataFrame(index=[0], geometry=[pg], crs=crs)


def bbox_to_gdf(bbox_tuple, crs, name_str=None, outfolder=None) -> gpd.GeoDataFrame:  #TODO: Deprecate
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


def bbox_to_wkt(minx, miny, maxx, maxy, srid=4326):
    """Creates a bounding box polygon from coordinates and returns WKT format."""
    wkt = f'SRID={srid};POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))'
    return wkt


def extract_row_bounds(row, field_name):
    return row[field_name], row.geometry.bounds


def fc_extents_to_dict(fc, field_name) -> dict:
    # Check if field_name exists in the GeoDataFrame
    gdf = open_fc_any(fc)
    epsg_list = [4269]

    epsg_list = gdf["EPSG_Code"].dropna().unique().tolist() + epsg_list if "EPSG_Code" in gdf.columns else epsg_list

    for epsg in epsg_list:
        gdf_prj = gdf.to_crs(epsg=epsg) if epsg != 4269 else gdf
        extents = dict(gdf_prj.apply(lambda row: extract_row_bounds(row, field_name), axis=1))
        yield extents, epsg


def dict_to_df(dictionary, index_field) -> pd.DataFrame:
    # Convert dictionary to DataFrame
    df = pd.DataFrame.from_dict(dictionary, orient='index', columns=[index_field, "Bounds"])
    # print(f'DF: {df}')
    return df


def extent_excel_from_fc(fc, fieldname, out_folder):
    # Write to Excel
    os.makedirs(out_folder, exist_ok=True)
    outpath = os.path.join(out_folder, f"{fieldname}_Extents_.xlsx")
    # Get extents from feature class
    for extents, epsg in fc_extents_to_dict(fc, fieldname):
        # Convert dictionary to DataFrame
        df = dict_to_df(extents, index_field=fieldname)
        df["westbc"] = df["Bounds"].apply(lambda x: x[0])
        df["southbc"] = df["Bounds"].apply(lambda x: x[1])
        df["eastbc"] = df["Bounds"].apply(lambda x: x[2])
        df["northbc"] = df["Bounds"].apply(lambda x: x[3])
        df["crs"] = epsg

        outpath = os.path.join(out_folder, f"{fieldname}_Extents_{epsg}.xlsx")
        if os.path.exists(outpath):
            os.remove(outpath)
        df.to_excel(outpath, sheet_name=f"{fieldname}_Extents_{epsg}", index=False)

    print(f"\tWrote extents to Excel: {outpath}")
    return outpath
