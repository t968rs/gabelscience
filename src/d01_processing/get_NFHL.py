import urllib
import json
import geopandas as gpd
import geojson
import os

nfhl_links = {"Effective":
                  {"S_FLD_HAZ_AR":
                       "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28?f=pjson"}}


# Function to convert ArcGIS Feature Layer to GeoJSON
def convert_to_geojson(arcgis_data):
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    for feature in arcgis_data['drawingInfo']['renderer']['uniqueValueInfos']:
        geojson_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",  # Assuming polygons; adjust if needed
                "coordinates": []  # Add coordinates here
            },
            "properties": feature
        }
        geojson['features'].append(geojson_feature)

    return geojson


def get_esri_data(fema_era, layer):
    link = nfhl_links[fema_era][layer]
    with urllib.request.urlopen(link) as url:
        data = json.loads(url.read().decode())
        geojson = convert_to_geojson(data)
        return geojson


if __name__ == '__main__':
    out_location = r"E:\nfhl"
    era, lyr = "Effective", "S_FLD_HAZ_AR"
    geojson = get_esri_data(era, lyr)

    outpath = os.path.join(out_location, f"{lyr}_{era}.geojson")
    with open(outpath, "w") as f:
        geojson.dump(outpath, f)
