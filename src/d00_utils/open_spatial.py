import geopandas as gpd
import os
import pyogrio


class FeatureClassReader:
    """
    A class to open feature classes from various file paths (including File Geodatabases).
    Uses GeoPandas under the hood.

    Example Usage:
        opener = FeatureClassReader("path/to/my.gdb/featureclass")
        gdf = opener.read()
    """
    def __init__(self, fc_path: str):
        """
        :param fc_path: The file path to the feature class or shapefile.
                        For a File Geodatabase, use: "path/to/my.gdb/featureclass".
        """
        self.fc_path = fc_path
        self.gdb = False
        self.gdb_dataset = False

        self.fc_name = None
        self.to_read = None

        self.__init_type()

    def __init_type(self):
        if ".gdb" in self.fc_path.lower():
            self.gdb = True
            # Split out the base GDB path and the layer name
            gdbpath, fc_name = os.path.split(self.fc_path)
            ds_test, suffix = self.fc_path.split(".gdb")
            ds_test = ds_test + ".gdb"

            # Decide what to pass to read_file based on ds_test vs fc_name
            if ds_test != fc_name:
                # e.g., "C:/Path/MyData.gdb/MyFeatureClass"
                self.gdb_dataset = True
                self.to_read = ds_test
                self.fc_name = fc_name
            else:
                # e.g., "C:/Path/MyData.gdb"
                self.to_read = gdbpath
        else:
            self.to_read = self.fc_path

    def __construct_read_dict(self, keeper_columns=None):
        read_dict = {"filename": self.to_read}
        if self.gdb:
            read_dict["engine"] = "pyogrio"
            read_dict["layer"] = self.fc_name
        if keeper_columns:
            read_dict["columns"] = keeper_columns

        return read_dict

    def read(self, keeper_columns=None, count_only=False):
        """
        Opens the feature class as a GeoDataFrame or returns the feature count.

        :param keeper_columns: List of column names to keep (optional).
        :param count_only: If True, returns the feature count instead of a GeoDataFrame.
        :return: Either a GeoDataFrame (if count_only=False) or an integer feature count (if count_only=True).
        """

        # Get reader options
        read_dict = self.__construct_read_dict(keeper_columns)

        try:
            gdf = gpd.read_file(**read_dict)
        except Exception as ex:
            print(f"Error reading from GDB path={self.to_read}, layer={self.fc_name}: {ex}")
            if count_only:
                return 0
            return None

        # Return either count or the full GeoDataFrame
        if count_only:
            length = len(gdf)
            del gdf
            return length
        else:

            # Remove shape columns
            if "geometry" in gdf.columns:
                keeper_columns = [c for c in gdf.columns if "shape" not in c.lower()]
                # print(f'\tKeepers: {keeper_columns}')
            return gdf[keeper_columns]


def get_gdb_layers(gdb_path: str) -> list:
    """
    Function to return a list of layers in a File Geodatabase.
    Args:
        gdb_path: str, path to the File Geodatabase.

    Returns:

    """

    # Test path before fiona import
    if not gdb_path:
        raise ValueError("gdb_path must be provided.")
    if not os.path.exists(gdb_path):
        raise FileNotFoundError(f"gdb_path does not exist: {gdb_path}")
    if ".gdb" not in gdb_path.lower():
        raise ValueError("gdb_path must be a path to a File Geodatabase (.gdb).")

    # Import
    import fiona

    # Return layer list
    layers = fiona.listlayers(gdb_path)
    return layers

def open_fc_any(fc_path, keeper_columns=None, count_only=False) -> gpd.GeoDataFrame:
    """
    Opens a feature class from a given file path and returns it as a GeoDataFrame.

    Parameters:
    fc_path (str): The file path to the feature class. This can be a path to a File Geodatabase (.gdb) or other supported file types.

    Returns:
    GeoDataFrame: A GeoDataFrame containing the data from the feature class.
    """
    reader = FeatureClassReader(fc_path)
    return reader.read(keeper_columns, count_only)