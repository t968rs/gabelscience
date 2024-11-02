import os
import threading
import typing as T
from dask.system import CPU_COUNT
import numpy as np
from numpy import dtype as npdtype
import xarray as xr
import rasterio.vrt
import rasterio
import rasterio.merge
import rasterio.windows
import rasterio.warp as rwarp
from rasterio.enums import Resampling
from dask.array import array
import dask.array as da
import dask.config
from dask.distributed import Client, LocalCluster, Lock
from dask.diagnostics import ProgressBar
from distributed.system import memory_limit
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from rasterio.rio.helpers import coords
from tqdm import tqdm
from rasterio.crs import CRS
import rioxarray as rxr
from src.d00_utils.files_finder import get_raster_list
from src.d00_utils.system import increment_file_naming, get_system_memory
from src.specs.raster_specs import create_raster_specs_from_path
from src.d00_utils.timer import timer
from src.d01_processing import gdal_helpers
import logging




rasterio_env = env = rasterio.Env(
    GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
    CPL_VSIL_CURL_USE_HEAD=False,
    CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF",
)

RASTERIO_CREATION_OPTIONS = {"tiled": True, "compress":'LZW', "bigtiff":'YES',
                             "blockxsize": 128, "blockysize": 128, "num_threads": 'ALL_CPUS'}

RASTERIO_RESAMPLING_LOOKUP = {
    "near": rasterio.enums.Resampling.nearest,
    "nearest": rasterio.enums.Resampling.nearest,
    "bilinear": rasterio.enums.Resampling.bilinear,
    "cubic": rasterio.enums.Resampling.cubic,
    "cubicspline": rasterio.enums.Resampling.cubic_spline,
    "lanczos": rasterio.enums.Resampling.lanczos,
    "mean": rasterio.enums.Resampling.average,
    "average": rasterio.enums.Resampling.average,
    "mode": rasterio.enums.Resampling.mode,
    "max": rasterio.enums.Resampling.max,
    "min": rasterio.enums.Resampling.min,
    "med": rasterio.enums.Resampling.med,
    "q1": rasterio.enums.Resampling.q1,
    "q3": rasterio.enums.Resampling.q3,
}


RASTERIO_MERGE_METHODS = {
    "max": rasterio.merge.copy_max,
    "min": rasterio.merge.copy_min,
    "sum": rasterio.merge.copy_sum,
}

CORE_COUNT = os.cpu_count()

log_path = f"./logs/{__name__}.log"
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename=log_path, filemode="a",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
dask.config.set(**{'array.slicing.split_large_chunks': False})


def reproject_array(block, ref_array):
    """Custom function to reproject a single block."""
    return block.rio.reproject_match(ref_array)

def get_union_coordinates(array_list: T.List[xr.DataArray]) -> T.Dict[str, np.ndarray]:
    # Determine the union of the coordinates
    x_coords = np.unique(np.concatenate([arr.coords['x'].values for arr in array_list]))
    y_coords = np.unique(np.concatenate([arr.coords['y'].values for arr in array_list]))
    return {'x': x_coords, 'y': y_coords}


def mosaic_arrays_by(path_list: T.List[os.PathLike],
                     by: str,
                     outfile: T.Union[str, os.PathLike],
                     chunk_pass: dict,
                     vrt_ref: T.Union[str, os.PathLike]) -> bool:
    """
    Mosaic Dask arrays by a given method.

    Parameters:
    path_list (List[os.PathLike]): List of file paths to the arrays to mosaic.
    by (str): Method to use for mosaicking ('max', 'min', 'mean').
    outfile (Union[str, os.PathLike]): Output file path for the mosaicked result.
    chunk_pass (dict): Chunking strategy for the input arrays.
    vrt_ref (Union[str, os.PathLike]): Path to the VRT reference file.

    Returns:
    bool: True if the operation is successful.
    """
    logger.info(f"Mosaicking {len(path_list)} arrays by {by}")
    logger.info(f"Path list: {path_list}")

    # Get system resources
    workers = int(CORE_COUNT / 2)
    sys_mem = get_system_memory()
    mem_per_worker = int(sys_mem * 0.75 / workers)
    print(f'\tWorkers: {workers}, Memory per worker: {mem_per_worker} GB')

    with rxr.open_rasterio(vrt_ref) as vrt_dataarray:
        vrt_attrs = vrt_dataarray.attrs
        vrt_crs = vrt_dataarray.rio.crs

    # Open the arrays with rioxarray
    array_list = [rxr.open_rasterio(path, chunks=chunk_pass) for path in path_list]

    # Create a blank DataArray with the union of the coordinates
    # blank_dataarray = create_blank_dataarray(array_list, -9999, vrt_attrs).chunk(chunk_pass)
    # blank_dataarray.rio.write_crs(vrt_crs, inplace=True)
    # logger.info(f"Blank dataarray with CRS and coordinates: {blank_dataarray}")

    # # Create a local Dask cluster and client
    # cluster = LocalCluster(n_workers=workers, memory_limit=f'{mem_per_worker}GB', processes=False)
    # client = Client(cluster)

    # Get max coords and bounds
    union_coords = get_union_coordinates(array_list)
    minx, miny, maxx, maxy = union_coords['x'][0], union_coords['y'][0], union_coords['x'][-1], union_coords['y'][-1]

    # Pad and reindex the arrays
    padded_arrays = [arr.rio.pad_box(minx, miny, maxx, maxy, constant_values=-9999) for arr in array_list]
    reindexed_arrays = [arr.reindex(x=union_coords['x'], y=union_coords['y'], method='nearest') for arr in padded_arrays]
    aligned_rasters = xr.align(*reindexed_arrays, join='outer')
    for arr in aligned_rasters:
        print(f'Shape: {arr.shape}')
        logger.info(f'Chunks: {arr.chunks}')

    # Combine rasters into a single dataset
    combined_raster = xr.concat(aligned_rasters, dim='band')
    combined_raster = da.stack(combined_raster, axis=0)

    if by == 'max':
        processed_array = da.nanmax(combined_raster, axis=0)
    elif by == 'min':
        processed_array = da.nanmin(combined_raster, axis=0)
    elif by == 'mean':
        processed_array = da.nanmean(combined_raster, axis=0)
    else:
        raise ValueError(f"Method {by} not supported")

    # Use Dask's ProgressBar to monitor the computation
    with ProgressBar():
        # Convert the Dask array back to an xarray DataArray
        logger.info(f"Processed array: {processed_array}")
        processed_array = xr.DataArray(processed_array, dims=['y', 'x'], coords=union_coords)
        processed_array.rio.write_crs(vrt_crs, inplace=True)
        processed_array.rio.write_nodata(-9999, inplace=True)
        logger.info(f"Processed array: {processed_array}")
        processed_array.rio.to_raster(outfile, tiled=True, compress='LZW', bigtiff='YES', lock=threading.Lock())

    # # Close the client and cluster
    # client.close()
    # cluster.close()

    return True

def mosaic_ndarray_by(ndarray, by):
    if by == 'max':
        result = np.nanmax(ndarray, axis=0)
    elif by == 'min':
        result = np.nanmin(ndarray, axis=0)
    elif by == 'mean':
        result = np.nanmean(ndarray, axis=0)
    elif by == 'median':
        result = np.nanmedian(ndarray, axis=0)
    else:
        result = ndarray

    return result

def adjust_windows_to_block_size(windows, block_size):
    adjusted_windows = []
    for window in windows:
        col_off, row_off = window.col_off, window.row_off
        width, height = window.width, window.height

        new_width = ((width + block_size - 1) // block_size) * block_size
        new_height = ((height + block_size - 1) // block_size) * block_size

        adjusted_window = rasterio.windows.Window(
            col_off=col_off,
            row_off=row_off,
            width=new_width,
            height=new_height
        )
        adjusted_windows.append(adjusted_window)
    return adjusted_windows

def estimate_array_size(array_obj: T.Union[rasterio.vrt.WarpedVRT, os.PathLike]) -> int:
    """
    Estimate the total file size of a Dask array from a VRT file.

    Parameters:
    vrt_path (str): Path to the VRT file.

    Returns:
    int: Estimated size of the array in bytes.
    """
    if isinstance(array_obj, rasterio.vrt.WarpedVRT):
        shape = array_obj.shape

    elif isinstance(array_obj, T.Union[str, os.PathLike]):
        with rasterio.open(array_obj) as src:
            shape = src.shape
    else:
        raise ValueError(f"Input must be a rasterio.vrt.WarpedVRT or a path to a raster file, \n\t{type(array_obj)}")
    dtype_size = npdtype('float32').itemsize
    total_elements = shape[0] * shape[1]
    estimated_size = total_elements * dtype_size

    return estimated_size

def adaptive_chunking(array_input: T.Union[da.Array, rxr.raster_array, rasterio.vrt.WarpedVRT]) -> da.chunk_types:
    """
    Determine the chunk size for a Dask array based on the total size of the array.
    @type array_input: object, str, os.PathLike
    """
    if isinstance(array_input, rasterio.vrt.WarpedVRT) or isinstance(array_input, T.Union[str, os.PathLike]):
        total_size = estimate_array_size(array_input) / 1e9
    else:
        total_size = array_input.size / 1e9

    # Get chunk size based on total size
    if total_size < 5:  # Less than 5 GB
        return {'x': 2048, 'y': 2048}
    elif total_size < 30:  # Less than 30 GB
        return {'x': 1024, 'y': 1024}
    else:  # More than 30 GB
        return {'x': 512, 'y': 512}

def get_max_bounds(raster_pathlist):
    txmin, tymin, txmax, tymax = None, None, None, None
    for path in raster_pathlist:
        with rasterio.open(path) as src:
            bounds = src.bounds
            xmin, ymin, xmax, ymax = bounds
            if txmin is None or xmin < txmin:
                txmin = xmin
            if tymin is None or ymin < tymin:
                tymin = ymin
            if txmax is None or xmax > txmax:
                txmax = xmax
            if tymax is None or ymax > tymax:
                tymax = ymax
    return txmin, tymin, txmax, tymax


class MosaicBy:

    def __init__(self,
                 output_folder: T.Union[str, os.PathLike],
                 input_folder: T.Union[str, os.PathLike] = None,
                 raster_pathlist: T.Union[list, T.Tuple[str, ...]] = None,
                 snap_raster: T.Union[str, os.PathLike] = None,
                 epsg: int = None,
                 sting_tolookfor: str = None,
                 mask: T.Union[str, os.PathLike] = None,
                 outname: str = "WSE",
                 cell_size: float = None):

        self.snap_raster = snap_raster
        self.snap_specs = None
        self.input_folder = input_folder

        self.epsg = epsg
        self.string_tolookfor = sting_tolookfor
        self.mask = mask
        self.cell_size = cell_size
        self.outname = outname
        self.target_crs = None

        if not raster_pathlist and not self.input_folder:
            raise ValueError("Either raster_pathlist or input_folder must be provided")
        if isinstance(raster_pathlist, list) or isinstance(raster_pathlist, os.PathLike):
            self.input_folder = None
        if isinstance(raster_pathlist, T.Union[str, os.PathLike]):
            raster_pathlist = [raster_pathlist]

        if self.input_folder:
            raster_pathlist = get_raster_list(self.input_folder, recursive=True)
        # self.crs_match = check_crs_match_from_list(raster_pathlist)

        self.epsg_info, self.vert_units = {}, {}
        self._pop_rasters_crs_info(raster_pathlist, vert_update=True)
        self.crs_match = True if len(set(self.epsg_info.values())) == 1 else False
        self._get_output_specs()

        self.output_folder = os.path.join(output_folder, f"{outname}_{epsg}")
        os.makedirs(self.output_folder, exist_ok=True)

    def _pop_rasters_crs_info(self, raster_pathlist, vert_update=False):
        pbar = tqdm(raster_pathlist, desc="Getting CRS info")

        for path in raster_pathlist:
            self.epsg_info[path] = create_raster_specs_from_path(path).epsg
            if vert_update:
                vertical_units = create_raster_specs_from_path(path).linear_units
                vertical_units = 'foot' if 'foot' in vertical_units else 'meter'
                self.vert_units[path] = vertical_units
            pbar.update(1)

    def _get_output_specs(self):
        if self.snap_raster:
            self.snap_specs = create_raster_specs_from_path(self.snap_raster)
            if not self.epsg:
                self.epsg = self.snap_specs.epsg
                self.target_crs = self.snap_specs.crs
            else:
                self.target_crs = CRS.from_epsg(self.epsg)
            self.cell_size = self.snap_specs.res
        else:
            if not self.cell_size:
                raise ValueError("Cell size must be provided")
            if not self.epsg:
                raise ValueError("EPSG must be provided")
            self.target_crs = CRS.from_epsg(self.epsg)
            for path, epsg in self.epsg_info.items():
                if epsg == self.epsg:
                    self.snap_specs = create_raster_specs_from_path(path)
                    self.snap_specs.update(res=self.cell_size, epsg=self.epsg, crs=self.target_crs)
                    break

    def _reproject_rasters(self, output_crs=None, notin_epsg=None):
        """
        Reproject all rasters to a new EPSG
        """
        if not output_crs:
            values_list = list(self.epsg_info.values())
            output_crs = max(set(values_list), key=values_list.count)
            logger.info(f"Most common EPSG: {output_crs}")
            print(f'\tMost common EPSG: {output_crs}')
            for v in set(self.vert_units.values()):
                print(f'\tLinear units: {v}')

        reprojected = []
        pbar = tqdm(notin_epsg, desc=f"Reprojecting {len(notin_epsg)} rasters")
        intmd_folder = os.path.join(self.output_folder, f'intmd_{output_crs}')
        print(f'\tOutput folder: {intmd_folder}')
        os.makedirs(intmd_folder, exist_ok=True)
        logger.info(f"Reprojecting {len(self.epsg_info)} rasters from "
                    f"{list(self.epsg_info.values())} to EPSG: {output_crs}")
        logger.info(f"Output folder: {intmd_folder}")

        ###### Multiprocessing Reproject ########
        futurelist = []
        in_out_lookup = {}
        with ProcessPoolExecutor() as executor:
            for path in notin_epsg:

                # Get the output linear units
                input_vert_units = self.vert_units.get(path)

                # File naming
                filename = os.path.basename(path)
                name, ext = os.path.splitext(filename)
                outpath = os.path.join(intmd_folder, f"{name}_{output_crs}_v{input_vert_units}{ext}")
                in_out_lookup[outpath] = path

                # Save the units
                self.vert_units[outpath] = input_vert_units
                self.vert_units.pop(path)

                # Project the raster
                future = executor.submit(gdal_helpers.gdal_warp, input_files=path,
                                         output_file=outpath,
                                         target_crs=output_crs,
                                         snap_specs=None,
                                         resampling_method="bilinear")
                futurelist.append(future)

            for future in as_completed(futurelist):
                outpath = future.result()
                logger.info(f"Reprojected: {outpath}")
                reprojected.append(outpath)
                pbar.update(1)

        # Update the rasters_info dictionary
        self._pop_rasters_crs_info(reprojected, vert_update=False)

        allpaths = list(self.epsg_info.keys())
        for path in allpaths:
            epsg = self.epsg_info.get(path)
            if epsg != output_crs:
                self.epsg_info.pop(path)
                if path in self.vert_units:
                    self.vert_units.pop(path)

        # Check if all rasters have been reprojected
        reproj_epsg_list = list(set(self.epsg_info.values()))
        self.crs_match = True if len(reproj_epsg_list) == 1 else False
        if not self.crs_match:
            logger.warning("CRS do not match after reprojecting")
            logger.info(f'{reproj_epsg_list}')
            for code in reproj_epsg_list:
                logger.info(f"Files with EPSG: {code}")
                logger.info([k for k, v in self.epsg_info.items() if v == code])

    def _convert_units(self, path, vert_units, out_vert_units):
        # Parameters
        input_epsg = self.epsg_info.get(path)
        out_folder = os.path.join(self.output_folder, f'intmd_{input_epsg}')
        os.makedirs(out_folder, exist_ok=True)

        # Convert units
        logger.info(f"Converting units from {vert_units} to {out_vert_units}")
        if out_vert_units == 'meter':
            multiple = 0.304833
        elif out_vert_units == 'foot':
            multiple = 3.280833
        else:
            multiple = 1
            print(f"Unknown linear units: {out_vert_units}")
        proj_array = rxr.open_rasterio(path, chunks={'x': 2048, 'y': 2048, 'band': 1})
        proj_array.rio.write_crs(input_epsg, inplace=True)
        proj_array = proj_array * multiple

        # Save the converted raster
        filename = os.path.basename(path)
        name, ext = os.path.splitext(filename)
        projected_path = os.path.join(out_folder, f"{name}_{input_epsg}_v{out_vert_units}{ext}")
        proj_array.rio.to_raster(projected_path, tiled=True, compress='LZW', bigtiff='YES', lock=threading.Lock())
        proj_array.close()

        # Update the rasters_info dictionary
        self._pop_rasters_crs_info([projected_path], vert_update=False)
        self.vert_units[projected_path] = out_vert_units
        self.epsg_info.pop(path)
        self.vert_units.pop(path)

        return projected_path

    def _mosaic_from_vrt(self, vrt_path, by='max'):

        # Get system resources
        workers = int(CORE_COUNT / 2)
        sys_mem = get_system_memory()
        mem_per_worker = int(sys_mem * 0.75 / workers)

        # Create a local Dask cluster and client
        cluster = LocalCluster(n_workers=workers, memory_limit=f'{mem_per_worker}GB', processes=False)
        client = Client(cluster)

        try:
            # Open the VRT with rasterio
            with rasterio.open(vrt_path, 'r') as vrtsrc:
                with rasterio.vrt.WarpedVRT(src_dataset=vrtsrc,
                                            crs=self.target_crs,
                                            resampling=Resampling.bilinear,
                                            tolerance=0.5) as vrt:
                    print(f'\nVRT: \n{vrt}')
                    chunking = adaptive_chunking(vrt)
                    print(f'Adaptive chunking: {chunking}')
                    dask_array = rxr.open_rasterio(vrt, chunks=chunking)
                    dask_array.rio.write_crs(self.epsg, inplace=True)
                    dask_array.rio.write_transform(vrt.transform, inplace=True)
                    dask_array.rio.write_nodata(-9999, inplace=True)
                    print(f'\nDask Array: \n{dask_array}')

                    # Mosaic the dask array
                    mosaic = mosaic_arrays_by(dask_array, by)
                    outpath = os.path.join(self.output_folder, f'{self.outname}_{self.epsg}_mosaic.tif')
                    outpath = increment_file_naming(outpath)

                    # Save the mosaic
                    mosaic.rio.to_raster(outpath, tiled=True, compress='LZW', bigtiff='YES', lock=Lock())
                    print(f'\nMosaic saved: {outpath}')
        except Exception as e:
            logger.error(f"Error: {e}")
            outpath = None
        finally:
            # Close the client and cluster
            client.close()
            cluster.close()

        return outpath

    def _mosaic_from_vrt2(self, vrt_path, by='max'):

        with rasterio.open(vrt_path) as src:
            profile = src.profile
            block_size = src.block_shapes[0][0] * 4
            windows = [window for ij, window in src.block_windows()]
            adjusted_windows = adjust_windows_to_block_size(windows, block_size)
            epsg = src.crs.to_epsg()
            outpath = os.path.join(self.output_folder, f'{self.outname}_{epsg}_merged_rio.tif')
            outpath = increment_file_naming(outpath)

            # Update the profile
            profile.update(blockxsize=block_size, blockysize=block_size, tiled=True, compress='LZW', bigtiff='YES')

            # Create the output file
            with rasterio.open(outpath, 'w', **profile) as dst:

                print(f"Processing {len(adjusted_windows)} windows")
                pbar = tqdm(adjusted_windows, desc=f"Mosaicking {len(adjusted_windows)} by {by}")

                # Locks
                read_lock, write_lock = threading.Lock(), threading.Lock()

                def mosaic_window(window):
                    with read_lock:
                        win_array = src.read(window=window, masked=True)

                    mosaic = mosaic_ndarray_by(win_array, by)

                    with write_lock:
                        dst.write(mosaic, window=window)
                    window, mosaic = None, None
                    return True

                with ThreadPoolExecutor() as executor:
                    with read_lock:
                        futures = [executor.submit(mosaic_window, window) for window in adjusted_windows]
                    for i, future in enumerate(as_completed(futures)):
                        finished_window = future.result()
                        pbar.update(1)
                        finished = i + 1
                        remaining = len(adjusted_windows) - finished
                        pbar.set_postfix({'Finished': finished, 'Remaining': remaining})
        return outpath

    def _mosaic_from_arrays(self, by='max'):

        # Create a VRT intermediate
        vrt_path = gdal_helpers.create_vrt(list(self.epsg_info.keys()), os.path.join(self.output_folder, f"mosaic_input.vrt"))
        vrt_epsg = create_raster_specs_from_path(vrt_path).epsg
        intmd_folder = os.path.join(self.output_folder, f'intmd_{vrt_epsg}')
        os.makedirs(intmd_folder, exist_ok=True)

        # Get info to pass to function
        chunks = adaptive_chunking(vrt_path)
        pathlist = list(self.epsg_info.keys())

        outpath = os.path.join(self.output_folder, f'{self.outname}_{vrt_epsg}_merged_rio.tif')
        outpath = increment_file_naming(outpath)

        # Create a Dask array
        try:
            mosaic_arrays_by(pathlist, by, outfile=outpath, chunk_pass=chunks, vrt_ref=vrt_path)
        except Exception as e:
            logger.error(f"Error: {e}")
            outpath = None

        return outpath


    def mosaic_by(self, by='max', statistics=False):
        """
        Mosaic rasters by a given method
        """

        # Check if all rasters are in the target CRS
        notin_epsg = [k for k, v in self.epsg_info.items() if v != self.epsg]
        already_epsg = [k for k, v in self.epsg_info.items() if v == self.epsg]

        if len(already_epsg) == len(self.epsg_info):
            logger.info("All rasters are already in the target CRS")
        else:

            # Reproject the rasters
            print("Reprojecting input rasters")
            logger.info("Not all rasters are in the target CRS")
            for path in notin_epsg:
                logger.info(f"Reprojecting: {path}")
                epsg = self.epsg_info.get(path)
                logger.info(f"Current EPSG: {epsg}")
            self._reproject_rasters(notin_epsg=notin_epsg)
            logger.info(f"Reprojected rasters: {len(self.epsg_info)}. Match: {self.crs_match}")

        # Get the output linear units
        out_lin_units = CRS.from_epsg(self.epsg).linear_units
        out_vert_units = 'foot' if 'foot' in out_lin_units else 'meter'

        # Convert units if necessary
        verts2convert = [k for k, v in self.vert_units.items() if v != out_vert_units]
        if verts2convert:
            pbar = tqdm(verts2convert, desc="Converting units")
            for path, vert_units in self.vert_units.items():
                if vert_units != out_vert_units:
                    pbar.set_postfix({'path': path, 'units': vert_units, 'out_units': out_vert_units})
                    outpath = self._convert_units(path, vert_units, out_vert_units)
                    pbar.update(1)

        # Mosaic the rasters
        print(f'\tMosaic {len(self.epsg_info)} rasters by {by}')
        logger.info(f"Mosaicking {len(self.epsg_info)} rasters by {by}")
        mosaic_path = self._mosaic_from_arrays(by=by)

        if not mosaic_path:
            print("Mosaic failed")
            return

        # Warp the mosaic
        outpath = os.path.join(self.output_folder, f'{self.outname}_{self.epsg}_mosaic.tif')
        outpath = increment_file_naming(outpath)
        gdal_helpers.gdal_warp(mosaic_path, outpath, self.epsg, self.snap_specs, resampling_method="bilinear")
        print(f'Exported: {outpath}')

        # Calc stats
        if statistics:
            for path in [outpath, ]:
                print(f'Calculating statistics for: {path}')
                with rasterio.open(path, 'r+', creation_options=RASTERIO_CREATION_OPTIONS) as src:
                    src.build_overviews([2, 4, 8, 16, 32, 64, 128], resampling=RASTERIO_RESAMPLING_LOOKUP.get('nearest'))
                    src.update_tags(ns='rio_overview', resampling='nearest')
                    print(f'\tBuilt overviews')
                    # Calculate statistics for each band
                    for band in src.indexes:
                        stats = src.statistics(band)
                        print(f"\tBand {band} statistics: {stats}")


if __name__ == "__main__":

    raster_snapper = r"E:\Iowa_2A\02_WORKING\South_Skunk_07080105\Grids\01_RAW_Mosaic\RAW_0_2pct_StormWaterDepth.tif"
    infolder = rf"E:\Iowa_2A\02_WORKING\South_Skunk_07080105\Grids\01_RAW_Mosaic\WSE_02pct\input_test"
    outfolder = rf"E:\Iowa_2A\02_WORKING\South_Skunk_07080105\Grids\01_RAW_Mosaic\WSE_02pct\output_test"
    limiting_string = None  # f"wse_{return_string}"
    output_epsg = 3417
    output_name = "WSE_02pct"
    mosaic_operator = "max"
    output_cell_size = 3.280833

    initialize = MosaicBy(outfolder, infolder, None,
                          raster_snapper, output_epsg, limiting_string,
                          outname=output_name,
                          cell_size=output_cell_size)
    timer(initialize.mosaic_by)(by=mosaic_operator, statistics=True)
