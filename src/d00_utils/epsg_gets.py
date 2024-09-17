import requests
from shapely.geometry import box
import geopandas as gpd


def get_epsg_code(in_data):
    import xarray as xr
    if isinstance(in_data, xr.DataArray):
        return in_data.rio.crs.to_epsg()
    elif isinstance(in_data, gpd.GeoDataFrame):
        return in_data.crs.to_epsg()


def get_gdfextent_centroid(epsg_code: [int, str]) -> (gpd.GeoDataFrame, tuple[float, float]):
    """
    Fetches the extent for a given EPSG code from the EPSG API, creates a GeoDataFrame
    with the bounding box, and plots it to an HTML file using folium.

    Parameters:
    - epsg_code: The EPSG code for which to fetch the extent.

    Returns:
    - A tuple containing the GeoDataFrame of the extent and the path to the HTML plot.
    """
    if isinstance(epsg_code, str):
        epsg_code = int(epsg_code)

    # Define the API URL
    url = f"https://spatialreference.org/ref/epsg/{epsg_code}/projjson.json"
    print(f'Fetching data for EPSG:{epsg_code} from \n   {url}')
    headers = {"Accept": "application/json"}

    # Send a GET request to the API
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        crs_box = data["bbox"]
        # Extract the bounding box coordinates
        west = crs_box["west_longitude"]
        south = crs_box["south_latitude"]
        east = crs_box["east_longitude"]
        north = crs_box["north_latitude"]
        retrieved_epsg = data["id"]["code"]
        if not retrieved_epsg == epsg_code:
            print(f"Retrieved EPSG code {retrieved_epsg} does not match requested EPSG code {epsg_code}")
            return None, None

        # Create a bounding box polygon
        bbox_polygon = box(west, south, east, north)

        # Create a GeoDataFrame with the polygon
        gdf = gpd.GeoDataFrame([{"epsg_code": epsg_code}], geometry=[bbox_polygon], crs="EPSG:4326")
        centroid = (gdf.geometry.centroid.x.iloc[0], gdf.geometry.centroid.y.iloc[0])
        return gdf, centroid
    else:
        print(f"Failed to fetch data for EPSG:{epsg_code}, Status Code: {response.status_code}")
        return None, None


if __name__ == "__main__":
    import warnings
    import os
    # Suppress the specific GeoPandas/PyProj warning about geographic CRS
    warnings.filterwarnings("ignore", message=".*Geometry is in a geographic CRS.*")

    epsg = 3417
    gdf_epsg, epsg_centroid = get_gdfextent_centroid(epsg)
    print(f"Centroid: {epsg_centroid}")
    m = gdf_epsg.explore(column="epsg_code", cmap="tab20", title=f"{epsg}_extent")
    output_loc = os.path.join(os.getcwd(), "test_data")
    output_html = os.path.join(output_loc, f"{epsg}_extent.html")
    m.save(output_html)
    gdf_projected = gdf_epsg.to_crs("EPSG:3857")
