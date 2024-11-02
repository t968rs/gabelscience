import rioxarray as rxr
import rasterio
import fiona

with fiona.open(site_boundary_path, "r") as shapefile:
    shapes = [feature["geometry"] for feature in shapefile]
    crs = shapefile.crs

# Use rioxarray to open, resample, reproject and clip raster data
with rasterio.open(prism_path) as src, rasterio.vrt.WarpedVRT(src, crs=crs) as vrt:
    prism = rxr.open_rasterio(vrt, masked=True).rio.clip(
        geometries=shapes,
        all_touched=True,
        from_disk=True,
    ).squeeze()