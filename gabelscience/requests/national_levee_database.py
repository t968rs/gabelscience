import pandas as pd
import requests
from typing import Dict, Any
import os

from zipfile import ZipFile, is_zipfile, BadZipFile
from dataclasses import dataclass, field
import geopandas as gpd
from src.d01_processing.merge_spatial import mergeGeoPackages as mGPKG
from src.d00_utils.open_spatial import FeatureClassReader


NLD_TYPES = {
    "floodwall": {
        "url": "https://levees.sec.usace.army.mil:443/api-local/floodwalls",
        "params": {
                    "embed": "geometry",
                    "format": "topo",
                    "props": "true"},
        "headers": {
            "accept": "application/json"
        }
    },
    "centerline": {
        "url": "https://levees.sec.usace.army.mil:443/api-local/geometries/query",
        "params": {
            "type": "centerline",
            "format": "geo",
            "props": "false",
            "coll": "true"
        },
        "headers": {"accept": "application/json"}
    },

    "dataset": {
        "url": "https://levees.sec.usace.army.mil:443/api-local/download/dataset/gpkg.zip?full=true&simple=false",
        "headers": {
            "accept": "application/json",
            "Content-Type": "application/json"},
        "params": {}
    }
}


class NLDRequest:

    def __init__(self, nld_type: str,
                 system_id_list: list,
                 output_folder: str,
                 method_type: str = "geojson",
                 refresh: bool = True,
                 **kwargs):

        if not nld_type or nld_type not in NLD_TYPES:
            raise ValueError(f"Please choose a valid NLD type from {NLD_TYPES.keys()}")

        self.base_url = NLD_TYPES[nld_type]["url"]
        self.headers = NLD_TYPES[nld_type]["headers"]
        self.params = NLD_TYPES[nld_type]["params"]
        self.method_type = method_type

        if system_id_list:
            self._init_system_id_lookup(system_id_list, refresh)

        if not output_folder:
            raise ValueError(f"Please offer an output folder")

        # Initialize mutable dictionaries
        self._system_id_lookup: Dict[str, Any] = {}
        self._downloads: Dict[str, str] = {}
        self._gpkg_paths: Dict[str, str] = {}
        self.merge_name: (str, None) = None

        # Output directory
        self.parent_folder = output_folder
        os.makedirs(output_folder, exist_ok=True)

        # Subfolder setup
        self.download_folder = os.path.join(output_folder, "nld_downloads")
        self.gpkg_folder = os.path.join(output_folder, "nld_gpkgs")
        self.merge_folder = os.path.join(output_folder, "nld_merged")
        if self.download_folder:
            os.makedirs(self.download_folder, exist_ok=True)
        if self.gpkg_folder:
            os.makedirs(self.gpkg_folder, exist_ok=True)
        if self.merge_folder:
            os.makedirs(self.merge_folder, exist_ok=True)

        # Initialize system ID lookup
        if system_id_list:
            self._init_system_id_lookup(system_id_list, refresh)

        # Handle additional keyword arguments
        for key, value in kwargs.items():
            if "name" in key:
                self.merge_name = value
        if not self.merge_name:
            self.merge_name = "nld_merged"


    def fetch_system_data(self):
        if not self._system_id_lookup:
            return None

        if not self.download_folder:
            print(f"Please offer an output folder")
            return None

        # Ensure out folder exists
        os.makedirs(self.download_folder, exist_ok=True)

        # Start the requests
        print("Starting requests...")
        for system_id, system_type in self._system_id_lookup.items():
            # Make the POST request
            params = self.params
            params["systemId"] = system_id
            response = requests.get(self.base_url, headers=self.headers, params=params)  # , data=system_id

            outpath = os.path.join(self.download_folder, f"{system_type}_{system_id}")

            # Check the response
            if response.status_code == 200:
                if self.method_type == "geojson":
                    outpath += ".geojson"
                    geojson_dict = response.json()
                    # print(f"GeoJSON dict: \n {geojson_dict}")
                    temp_gdf = gpd.GeoDataFrame.from_features(geojson_dict["features"])
                    temp_gdf["system_id"] = system_id
                    print(f"Columns: \n {temp_gdf.columns}")
                    if "geometry" not in temp_gdf.columns:
                        continue
                    temp_gdf.set_crs(crs="EPSG:4269", inplace=True)
                    temp_gdf.to_file(outpath, driver="GeoJSON")

                self._downloads[system_id] = outpath
                print("\tFile downloaded successfully.")

            else:
                print(f"Request failed with status code {response.status_code}")
                print(response.text)

        # self._unzip_downloads()

    def merge_system_data(self):
        if not self._gpkg_paths and self.method_type != "geojson":
            return None
        elif self.method_type != "geojson":
            return self._merge_gpkgs()
        else:
            print("Merging GeoJSONs...")
            self._merge_geojsons()

    #
    # Private Methods
    #

    def _merge_geojsons(self):
        if not self._downloads:
            return None

        merged_path = os.path.join(self.merge_folder, self.merge_name)
        if not ".geojson" in merged_path:
            merged_path += ".geojson"

        merged_gdf = gpd.GeoDataFrame()
        for system_id, download_path in self._downloads.items():
            print(f'\tAdding {system_id} to merge: {merged_path}')
            temp_gdf = gpd.read_file(download_path)

            merged_gdf = gpd.GeoDataFrame(pd.concat([merged_gdf, temp_gdf], ignore_index=True))

        merged_gdf.to_file(merged_path, driver="GeoJSON")
        merged_gdf.to_file(merged_path.replace(".geojson", ".shp"), driver="ESRI Shapefile")
        return merged_path

    def _merge_gpkgs(self):
        if not self._gpkg_paths:
            return None

        geo_merger = mGPKG(self.merge_folder)
        geo_merger.import_gpkg(self._gpkg_paths)

        try:
            geo_merger.merge_geopackages(self.merge_name)

            return geo_merger.output_path
        except Exception as ex:
            print(f"Error merging gpkgs: {ex}")
            return None


    def _unzip_downloads(self):
        for system_id, download_path in self._downloads.items():
            gpkg_path = self._unzip_a_gpkg(download_path, self.download_folder)
            self._gpkg_paths[system_id] = gpkg_path
            # os.remove(download_path)  # TODO remove intermediates + rmdir

    @staticmethod
    def _unzip_a_gpkg(zip_path: str, extract_to: str) -> str:
        """
        Unzips a .zip file containing a .gpkg into the specified folder.

        Args:
            zip_path (str): Path to the .zip file.
            extract_to (str): Directory where the .gpkg should be extracted.

        Returns:
            str: Path to the extracted .gpkg file.
        """

        try:
            if not is_zipfile(zip_path):
                raise BadZipFile(f"The file {zip_path} is not a valid ZIP file.")

            with ZipFile(zip_path, 'r') as zipf:
                zipf.extractall(extract_to)

            for root, dirs, files in os.walk(extract_to):
                for file in files:
                    if file.endswith(".gpkg"):
                        return os.path.join(root, file)

            raise FileNotFoundError("No .gpkg file found in the unzipped contents.")

        except BadZipFile as e:
            print(f"Error: {e}")
            # os.remove(zip_path)  # Delete the corrupted file
            raise


    def _init_system_id_lookup(self, input_list, refresh: bool = True):
        new_system_ids = {}
        if not input_list:
            return {}
        elif isinstance(input_list, str):
            input_list = [input_list]

        for system_id in input_list:
            new_system_ids[self._format_system_id(system_id)] = "System"

        self._system_id_lookup = new_system_ids if refresh else self._system_id_lookup.update(new_system_ids)

    @staticmethod
    def _format_system_id(input_id):

        if isinstance(input_id, int):
            return str(input_id)
        elif isinstance(input_id, float):
            return str(int(input_id))
        else:
            return input_id


if __name__ == "__main__":


    out_dir = r"Z:\Shell_Rock\02_WORKING\shellrock_mapping\levee"
    input_shp_path = r"Z:\Shell_Rock\02_WORKING\shellrock_mapping\levee\levee_systems_3417.shp"

    reader = FeatureClassReader(input_shp_path)
    gdf = reader.read()
    print(f"Input COlumns: \n{gdf.columns} \n")

    in_sys_id_list = gdf["id"].tolist()
    print(in_sys_id_list)

    nld_req = NLDRequest("centerline", in_sys_id_list, out_dir, kwargs={"outname": "levee_systems_ShellRock"})
    nld_req.fetch_system_data()
    nld_req.merge_system_data()




