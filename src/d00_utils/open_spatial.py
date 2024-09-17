import geopandas as gpd
import os


def open_fc_any(fc_path):
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
            gdf = gpd.read_file(ds_test, driver='FileGDB', layer=fc)
        else:
            # Otherwise, read from the geodatabase path
            print(f'GDB Path: {gdbpath}, FC: {fc}')
            gdf = gpd.read_file(gdbpath, driver='FileGDB', layer=fc)
    else:
        # If not a File Geodatabase, read the file directly
        gdf = gpd.read_file(fc_path)
    return gdf
