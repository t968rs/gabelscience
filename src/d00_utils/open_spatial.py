import geopandas as gpd
import os


def open_fc_any(fc_path, keeper_columns=None, count_only=False):
    """
    Opens a feature class from a given file path and returns it as a GeoDataFrame.

    Parameters:
    fc_path (str): The file path to the feature class. This can be a path to a File Geodatabase (.gdb) or other supported file types.

    Returns:
    GeoDataFrame: A GeoDataFrame containing the data from the feature class.
    """
    # Check if the file path points to a File Geodatabase
    if ".gdb" in fc_path:
        gdbpath, fc = os.path.split(fc_path)
        ds_test, suffix = fc_path.split(".gdb")
        ds_test = ds_test + ".gdb"
        print(f"DS Test: {ds_test}")
        # If the dataset test path is not equal to the feature class, read from the dataset test path
        if ds_test != fc:
            print(f'GDB Path: {ds_test}, FC: {fc}')
            to_read = ds_test
        else:
            # Otherwise, read from the geodatabase path
            print(f'GDB Path: {gdbpath}, FC: {fc}')
            to_read = gdbpath
        gdf = gpd.read_file(to_read, driver='FileGDB', layer=fc, columns=keeper_columns)
    else:
        # If not a File Geodatabase, read the file directly
        try:
            gdf = gpd.read_file(fc_path, columns=keeper_columns)
        except:
            os.remove(fc_path)
            if count_only:
                return 0
            else:
                return None

    if count_only:
        feature_count = len(gdf)
        gdf = None
        return feature_count
    else:
        return gdf
