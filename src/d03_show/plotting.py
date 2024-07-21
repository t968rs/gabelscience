import os
import folium
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from shapely.geometry import Point
import json


class Plotting:
    def __init__(self, input_data, folder=None, title=None):
        if folder is None:
            folder = os.path.join(os.getcwd(), "plotting_results")
        os.makedirs(folder, exist_ok=True)
        self.folder = folder
        if title is None:
            title = "results"
        self.map_config_path = os.path.join(self.folder, f"{self.title}_map_config.json")
        self.title = title
        self.map_path = os.path.normpath(os.path.join(self.folder, f"{self.title}.html"))
        self.indata = input_data

    def _load_or_initialize_map_config(self):
        if os.path.exists(self.map_config_path):
            with open(self.map_config_path, 'r') as file:
                return json.load(file)
        else:
            return {"layers": []}

    def _save_map_config(self, map_config):
        with open(self.map_config_path, 'w') as file:
            json.dump(map_config, file, indent=4)

    @staticmethod
    def _update_map_with_layers(m, map_config):
        for layer in map_config['layers']:
            if layer['type'] == 'GeoJson':
                folium.GeoJson(layer['data_source'], style_function=lambda x:
                {'fillColor': 'blue', 'color': 'blue', 'weight': 1, 'fillOpacity': 0.6}).add_to(m)
            # Add other layer types as needed

    def plot_to_html(self):
        map_config = self._load_or_initialize_map_config()
        # Update map configuration with new layer
        if isinstance(self.indata, gpd.GeoDataFrame):
            self.indata.to_file(os.path.join(self.folder, f"{self.title}_layer.geojson"), driver='GeoJSON')
            new_layer_config = {"type": "GeoJson",
                                "data_source": os.path.join(self.folder, f"{self.title}_layer.geojson")}
            map_config['layers'].append(new_layer_config)
        elif isinstance(self.indata, xr.DataArray):
            # Convert DataArray to GeoDataFrame or similar, then to GeoJSON as above
            # This part needs to be implemented based on how you want to convert DataArray to GeoJSON
            pass

        self._save_map_config(map_config)

        # Generate or update the map
        if os.path.exists(self.map_path):
            m = folium.Map(location=[0, 0], zoom_start=2)  # Default location, should be updated based on layers
        else:
            m = folium.Map(location=[0, 0], zoom_start=2)

        self._update_map_with_layers(m, map_config)

        # Save the map as an HTML file
        m.save(self.map_path)
        print(f"Map saved to {self.map_path}")


def plot_map_data_to_html(data: object, folder: str = None, title: str = None):
    plotter = Plotting(data, folder, title)
    plotter.plot_to_html()
