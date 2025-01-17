import os

import fiona
import geopandas as gpd
import pandas as pd


class mergeGeoPackages:

    _target_folder: str = None
    _output_driver: str = "Esri Shapefile"
    _output_path: str = None

    _system_id_index: list = []
    _input_gpkg_paths: list = []

    def __init__(self, target_folder: str,
                 output_driver: str = "Esri Shapefile"):
        if not target_folder:
            raise ValueError("Target GeoPackage path is required.")
        self._target_folder = target_folder
        os.makedirs(
            target_folder,
            exist_ok=True)

        self._output_driver = output_driver


    def import_gpkg(self, gpkg_path: (str, list, dict)):
        if isinstance(gpkg_path, str):
            self._input_gpkg_paths.append(gpkg_path)
        elif isinstance(gpkg_path, list):
            self._input_gpkg_paths.extend(gpkg_path)
        elif isinstance(gpkg_path, dict):
            self._input_gpkg_paths.extend(gpkg_path.values())
            self._system_id_index.extend(gpkg_path.keys())
        else:
            raise ValueError("Invalid input type. Expected str, list, or dict.")


    def merge_geopackages(self, output_filename: str = None):

        # Init empty geodataframe
        overall_gdf = gpd.GeoDataFrame()

        for i, gpkg_path in enumerate(self._input_gpkg_paths):
            print(f"Reading GeoPackage: {gpkg_path}")
            systemd_idx = self._system_id_index[i] if self._system_id_index else i

            # Call the merge
            merged_gdf, sys_id = merge_gpkg_layers(gpkg_path, systemd_idx)

            # Perform merge
            overall_gdf = gpd.GeoDataFrame(pd.concat([overall_gdf, merged_gdf], ignore_index=True))

        if not overall_gdf.empty:
            self._save_gdf(overall_gdf, output_filename)

    @property
    def output_path(self):
        return self._output_path


    def _save_gdf(self, gdf: gpd.GeoDataFrame, output_filename: str):
        # Save the merged GeoDataFrame as a new GeoPackage

        # Handle filename
        if not output_filename:
            output_filename = "merged_from_GPKG.shp"
        elif "." not in output_filename:
            output_filename += ".shp"

        outpath = os.path.join(self._target_folder, output_filename)

        # Write output
        gdf.to_file(outpath, driver=self._output_driver, mode="w")
        print(f"Merged layers saved to {outpath}")

        self._output_path = outpath



def merge_gpkg_layers(gpkg_path: str, *args):
    """
    Merges all layers in a GeoPackage into a single gdf.

    Args:
        gpkg_path (str): Path to the GeoPackage (.gpkg).
    """
    # Read all layers from the GeoPackage
    layers = gpd.io.file.fiona.listlayers(gpkg_path)
    merged_gdf = gpd.GeoDataFrame()


    for layer in layers:
        print(f"\tReading layer: {layer}")
        gdf = gpd.read_file(gpkg_path, layer=layer)
        merged_gdf = gpd.GeoDataFrame(pd.concat([merged_gdf, gdf], ignore_index=True))

    return merged_gdf, args

