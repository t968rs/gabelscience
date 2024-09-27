"""
A class to clip raster data using a polygon shapefile. Can optionally export a "mask raster" with the same extent as the
input raster, but with the values set to 1 where the input raster intersects the polygons, and 0 elsewhere.
"""
import typing as T
import pandas as pd
import os
import geopandas as gpd
import rasterio.crs
import xarray as xr
import rioxarray as rioxr
from src.d00_utils.regular_grids import create_regular_grid
from src.d00_utils.get_ext_from import get_extension, get_spatial_type
from src.d00_utils.bounds_convert import bbox_to_gdf
from src.d00_utils.check_crs_match import check_crs_match_from_list
from src.d00_utils.open_spatial import open_fc_any
from src.d01_processing import export_raster
from src.specs import raster_specs, literals
from geocube.api.core import make_geocube
import rasterio.features


def get_bounds_intersection(kwargs: T.Dict[str, T.Any]) -> T.Tuple[float, float, float, float]:
    print(f'Finding bounds intersection...')
    out_loc = kwargs.get("out_loc", os.getcwd() + "/test_folder/")
    os.makedirs(out_loc, exist_ok=True)
    out_path = out_loc + "bounds.shp"

    paths = [path for k, path in kwargs.items() if "path" in k]
    print(f'Paths: {paths}')
    if not check_crs_match_from_list(paths):
        raise ValueError("CRS mismatch in input files")

    bboxes = {}
    crs = None
    for i, path in enumerate(paths):
        extension = get_extension(path)
        s_type = get_spatial_type(extension)
        # print(f'File: {path}, Type: {s_type}, Ext: {extension}')
        if s_type == "raster":
            print(f"Raster file: {path}")
            with rasterio.open(path) as src:
                bboxes[f"path_{i}"] = src.bounds
                if not crs:
                    crs = src.crs
        else:
            print(f'Vector file: {path}')
            gdf = open_fc_any(path)
            if not crs:
                crs = gdf.crs
            bboxes[f"path_{i}"] = gdf.total_bounds

    bbox_pg = {}
    for k, bbox in bboxes.items():
        gdf = bbox_to_gdf(bbox, crs=crs)
        outpath_ = out_loc + f"bounds_{k}.shp"
        gdf.to_file(outpath_)
        bbox_pg[k] = gdf.geometry

    all_pg = [v for v in list(bbox_pg.values())]
    print(f'-- {len(all_pg)} bounds polygons')
    all_geo = pd.concat(all_pg)
    pg_intersection = all_geo.union_all().intersection(all_geo.union_all())
    intsct_bounds = gpd.GeoSeries(pg_intersection).total_bounds

    return intsct_bounds


def get_return_from_string(input_string: str) -> T.Any:
    """
    Converts a string to a Python object.

    Parameters:
        input_string (str): The string to convert.

    Returns:
        Any: The Python object created from the string.
    """

    parts = input_string.split("_")
    pct_part = None
    for i, part in enumerate(parts):
        if "pct" in part:
            pct_part = part
            break
    if pct_part:
        part_index = parts.index(pct_part)
        pct_part = pct_part.replace("pct", "")

        if pct_part.isnumeric():
            if pct_part == "02":
                prev_part = parts[part_index - 1]
                if prev_part.isnumeric():
                    pct_part = f"{prev_part}{pct_part}"
            return float(f"0.{pct_part}")
        else:
            return None


class ClipAraster:
    def __init__(self, input_pg_file: T.Union[os.PathLike, str],
                 epsg_code: int,
                 raster_value: float,
                 rawraster: T.Union[os.PathLike, str],
                 ops_type: str,
                 alternate_outname: T.Union[None, str],
                 all_touching: bool):
        """
        Initializes the ClipAraster object with the provided parameters and asserts the operation type.

        Parameters:
            input_pg_file (str): Path to the input polygon shapefile.
            epsg_code (int): EPSG code for the coordinate reference system.
            raster_value (float): Value to use for the raster data.
            rawraster (str): Path to the raw raster file to be processed.
            ops_type (str): Type of operation to perform.
            alternate_outname (str): Alternate filename for the output raster.
            all_touching (bool): Flag to include all touching pixels in the clip operation.
        """

        self.input_pg_file = input_pg_file
        self.epsg_code = epsg_code
        self.crs = f"EPSG:{epsg_code}"
        self.raster_value = raster_value
        self.raw_raster = rawraster
        self.target_bounds = get_bounds_intersection({"path1": input_pg_file, "path2": rawraster})
        self.ops_type = ops_type
        if alternate_outname is not None:
            self.alternate_outname = alternate_outname
        else:
            self.alternate_outname = None
        self.alltouched = all_touching

        self.output_ds = xr.Dataset()

        assert self.ops_type in ['make mask', 'clip only', "both"]
        self.chunk_return = {"x": 2048, "y": 2048}
        self.out_paths: T.Dict[str, T.Union[T.Any, None]] = {"ones_mask": None, "clipped_raster": None,
                                                             "output_folder": None}
        self._init_outputs()
        self.specs = self._init_raster_specs()
        self.return_period = get_return_from_string(os.path.split(self.raw_raster)[1])
        print(f'\n Return period: {self.return_period}')

    def _init_outputs(self):
        """
        Initializes output paths and filenames based on the raw raster path and the alternate output name.
        """
        base, filename = os.path.split(self.raw_raster)
        name = filename.split(".")[0]
        if not self.alternate_outname:
            self.out_paths["clipped_raster"] = os.path.join(base, f"{name}_CLIPPED.tif")
            self.out_paths["ones_mask"] = os.path.join(base, f"{name}_ONES_MASK.tif")
        else:
            self.out_paths["clipped_raster"] = os.path.join(base, self.alternate_outname + ".tif")
            self.out_paths["ones_mask"] = os.path.join(base, f"{self.alternate_outname}_ONES_MASK.tif")
        self.out_paths["output_folder"] = base

    def _init_raster_specs(self):
        """
        Initializes the raster specifications for the input raster dataset.
        """

        return {"input_raster": raster_specs.create_raster_specs_from_path(self.raw_raster, calc_valid_area=True)}

    @staticmethod
    def clip_it(ds: xr.Dataset,
                gdf: gpd.GeoDataFrame,
                crs: T.Union[str, rasterio.crs.CRS], alltouched: bool) -> xr.Dataset:
        """
        Clips the raster dataset using the provided GeoDataFrame and CRS.

        Parameters:
            ds (xarray.Dataset): The raster dataset to clip.
            gdf (geopandas.GeoDataFrame): The GeoDataFrame containing the clipping polygons.
            crs (str): The coordinate reference system to use for the clipping.
            alltouched (bool): Flag to include all touching pixels in the clip operation.

        Returns:
            xarray.Dataset: The clipped raster dataset.
        """

        ds.rio.write_crs(crs)

        rasterized = ds.rio.clip(gdf.geometry.values, crs=crs, all_touched=alltouched, from_disk=True, drop=False)

        return rasterized

    def process_clip(self) -> xr.Dataset:

        # Open the raster as dataset
        ds = rioxr.open_rasterio(self.raw_raster, chunks=self.chunk_return,
                                 lock=False, band_as_variable=True)
        print(ds)
        ds = ds.rename_vars({"band_1": "clipped_raster"})
        variable0 = [v for v in ds.data_vars][0]
        nodata_value = ds[variable0].rio.nodata
        print(f' {variable0} nodata: {nodata_value}')
        ds.rio.write_crs(self.crs, inplace=True)
        ds[variable0] = ds[variable0].round(3)

        # Process ones mask option
        if self.ops_type == "both":
            ones_array = xr.ones_like(ds[variable0], dtype="int8")
            ones_array = ones_array.rio.write_crs(self.crs, inplace=True)
            ones_array = ones_array.where(ones_array == 1, 0)
            ones_array.rio.write_nodata(0, inplace=True)
            ds["ones_mask"] = ones_array

        intersected = self.import_polygons()

        # Call the clip
        rasterized = self.clip_it(ds, intersected, intersected.crs, alltouched=self.alltouched)
        rasterized[variable0].rio.write_nodata(nodata_value, inplace=True)
        print(f' Created masked array\n{rasterized}')

        return rasterized

    def import_polygons(self):
        # Read Polygon Shapefile
        gdf = open_fc_any(self.input_pg_file)

        # QUery gdf
        if "FLD_ZONE" in gdf.columns and self.return_period:
            if self.return_period == 0.01:
                gdf = gdf.loc[gdf["FLD_ZONE"] != "X"]

        gdf = gdf.to_crs(self.crs).loc[:, ["geometry"]]

        polygon_area_pct = round(gdf.area.sum() / self.specs["input_raster"].valid_area, 2)
        if polygon_area_pct > 1.1:
            print(f'\nPolygons are {polygon_area_pct} times larger than raster area')
            gdf.geometry = gdf.geometry.clip_by_rect(*self.target_bounds)
        print(f"\nRead polygon shapefile\n {gdf.columns}")

        # Split it up for processing
        grid_gdf, n_cells, grid_features = create_regular_grid(gdf, n_cells=(2, 2),
                                                               epsg_code=self.epsg_code,
                                                               overlap=False)
        grid_gdf.to_file(os.path.join(os.path.split(self.input_pg_file)[0], "grid.shp"), index=True)
        intersected = grid_gdf.overlay(gdf, how='intersection', keep_geom_type=True)
        intersected = intersected.explode(index_parts=False).loc[:, ["geometry"]]
        intersected["values"] = 1
        print(f' Prepped polygons\n {intersected.columns}')
        intersected.to_file(os.path.join(os.path.split(self.input_pg_file)[0], "intersect.shp"))
        return intersected

    def get_polygon_as_ones_mask(self):

        intersected = self.import_polygons()

        # Call the clip
        with rioxr.open_rasterio(self.raw_raster) as src:
            rasterized = make_geocube(vector_data=intersected, measurements=["values"],
                                      like=src)
            for var in rasterized.rio.vars:
                rasterized = rasterized.rename_vars({var: "ones_mask"})
                rasterized["ones_mask"].rio.write_nodata(0, inplace=True).astype("int8")
            # rasterized = ones_array.to_dataset(name="ones_mask")
            print(f' Created masked ds\n{rasterized}')
            return rasterized

    def rasterizer(self):

        print(self.specs["input_raster"].__repr__())

        if self.ops_type == "make mask":
            self.output_ds = self.get_polygon_as_ones_mask()
        elif self.ops_type == "clip only":
            self.output_ds = self.process_clip()
        elif self.ops_type == "both":
            self.output_ds = self.process_clip()
        else:
            raise ValueError("Invalid operation type")

        outpaths = {}
        if self.output_ds:
            for var in self.output_ds.rio.vars:
                print(f'Exporting {var}')
                outpath = self.out_paths[var]
                export_raster.export_raster(self.output_ds[var], self.out_paths[var])
                outpaths[var] = outpath
        return outpaths


if __name__ == "__main__":
    input_shape = r"E:\pinal_2023\02_mapping\rasters\05_WSE_Processed\masks\SFHA_Pinal_Prelim_6405.shp"
    epsg = 6405
    value = 0.01
    operation_type = "clip only"
    exact_or_touched = True

    alt_output_filename = "test"
    folder = False  # r"E:\Iowa_1A\02_mapping\Grids_Lower_Wap\03_DRAFT_DFIRM"
    raster_file = r"E:\pinal_2023\02_mapping\rasters\05_WSE_Processed\WSE_01pct_West_Zone_A1.tif"
    if folder:
        for gridtype in ["Depth", "WSE"]:
            raster_file = rf"DRAFT_DFIRM_{gridtype}_01pct.tif"
            if os.path.isfile(raster_file):
                raise ValueError(f"Raster path is file: {raster_file}")
            raster_folder = raster_file
            # ["make mask", "clip only", "make mask"]
            alt_output_filename = None  # f"DRAFT_DFIRM_WSE_01pct"

            init = ClipAraster(input_shape, epsg, value, raster_file, operation_type, alt_output_filename,
                               exact_or_touched)
    else:
        init = ClipAraster(input_shape, epsg, value, raster_file, operation_type, alt_output_filename, exact_or_touched)
        outputs = init.rasterizer()
        print(f'Outputs')
        for k, v in outputs.items():
            print(f'{k}: {v}')
