import os
import dask.array as da
import rioxarray as rioxr
import xarray as xr
from dask import delayed
from dask.diagnostics import ProgressBar, ResourceProfiler
from dask.distributed import Client, LocalCluster
from tqdm import tqdm
from src.d00_utils import create_regular_grid
from src.d03_show.cplotting import plot_map_data_to_html as plot_to_html


def dask_progress_bar(min_time: int):
    # Initialize ProgressBar with the total number of tasks
    progress_bar = ProgressBar(min_time)
    progress_bar.register()
    return progress_bar


def update_tqdm_pbar(pbar_in, desc_update, number, append=False):
    if append:
        desc = pbar_in.desc
        pbar_in.set_postfix(desc=f", {desc_update}")
        pbar_in.update(number)
    else:
        pbar_in.set_description(desc_update)
        pbar_in.update(number)


class DaskReproject:

    def __init__(self, source_da, target_da, crs=None, window_gdf=None, chunk_win=None):
        if chunk_win is None:
            chunk_win = {"x": 2048, "y": 2048}
        self.source_da = source_da
        self.target_da = target_da
        self.crs = crs
        self.window_gdf = window_gdf
        self.chunk_win = chunk_win
        self._init_array_props()
        if window_gdf is None:
            _bounds = self.target_da.rio.bounds()
            print(f'Bounds: {_bounds}')
            self.window_gdf, n_cells, total_windows = create_regular_grid(bounds=_bounds,
                                                                          n_cells=(4, 4), epsg_code=self.crs.to_epsg())
            self.window_gdf.to_file(r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\window_gdf.shp")
            print(f'Bounds splitting: {n_cells}, number of windows: {total_windows}')
        self.window_gdf.reset_index(drop=True, inplace=True)
        self.errors = {}

    def _init_array_props(self):
        # Get the dimensions of the source DataArray
        self.source_dims = self.source_da.dims
        self.source_shape = self.source_da.shape
        self.source_chunks = self.source_da.chunks
        # Get the dimensions of the target DataArray
        self.target_dims = self.target_da.dims
        self.target_shape = self.target_da.shape
        self.target_chunks = self.target_da.chunks
        if self.crs is None:
            self.crs = self.target_da.rio.crs
        # Get the window size for each chunk

    # noinspection PyUnresolvedReferences
    def get_a_chunk(self, bounds, id_pass=None):
        try:
            box_arr = self.source_da.rio.clip_box(*bounds, auto_expand=True, auto_expand_limit=2)
        except rioxr.exceptions.NoDataInBounds as nd:
            self.errors.setdefault("NoDataInBounds", []).append(id_pass)
            box_arr = None
        except rioxr.exceptions.OneDimensionalRaster as odr:
            self.errors.setdefault("OneDimensionalRaster", []).append(id_pass)
            box_arr = None
        return box_arr

    @staticmethod
    @delayed
    def reproject_match_chunk(source_chunk, target_chunk, id_pass=None):

        """
        Reproject a chunk of a DataArray to match the target template's grid.

        Parameters:
        - source_chunk: The chunk of the source DataArray.
        - target_template: The target DataArray template to match.

        Returns:
        - The reprojected chunk as a DataArray.
        """

        # Assuming source_chunk is a DataArray with spatial dimensions (like x, y)
        # and it has a crs attribute set.
        if isinstance(source_chunk, xr.DataArray) and isinstance(target_chunk, xr.DataArray) and source_chunk.size:
            try:
                re_proj_chunk = source_chunk.rio.reproject_match(target_chunk)
                # Ensure the output has the correct dimensions
                if re_proj_chunk.shape != (len(target_chunk.y), len(target_chunk.x)):
                    re_proj_chunk = re_proj_chunk.rio.pad_box(*target_chunk.rio.bounds())
                return re_proj_chunk
            except Exception as e:
                print(f"Error during reprojecting (ln 64): {e}")
                # Return a placeholder array with the correct dimensions filled with np.nan
                return xr.full_like(target_chunk, -9999)
        else:
            # Return a placeholder array if inputs are not as expected
            return xr.full_like(target_chunk, -9999)

    def apply_reproject_match(self) -> xr.DataArray:
        """
        Apply reproject_match over chunks of a source DataArray to match the target DataArray.

        Parameters:
        - source_da: The source DataArray to be reprojected.
        - target_da: The target DataArray to match.

        Returns:
        - A reprojected source DataArray matching the target DataArray grid.
        """

        with (dask_progress_bar(min_time=1) as pbar, ResourceProfiler(dt=1) as profiler):
            # Placeholder for delayed results
            delayed_results = []
            # Apply function using xr.apply_ufunc, wrapped in delayed for parallel execution
            for index, feature in self.window_gdf.iterrows():
                grid_id = index if "grid_id" not in feature else feature["grid_id"]
                bounds = feature["geometry"].bounds
                s_chunk = self.get_a_chunk(bounds, grid_id)
                t_chunk = self.get_a_chunk(bounds, grid_id)
                if s_chunk is None or t_chunk is None:
                    continue
                s_chunk = s_chunk.rio.write_crs(self.crs)
                t_chunk = t_chunk.rio.write_crs(self.crs).rio.pad_box(*bounds, constant_values=-9999)
                # plot_to_html(s_chunk)
                # Apply function using delayed for parallel execution
                delayed_result = self.reproject_match_chunk(s_chunk, t_chunk, id_pass=grid_id)
                # Store the delayed result
                if delayed_result is not None:
                    delayed_results.append(delayed_result)
                else:
                    pbar.update(1)

            # print errors
            id_col = index if "grid_id" not in self.window_gdf.columns else "grid_id"
            not_error = {}
            for error_type, error_list in self.errors.items():
                deduped_errors = list(set(error_list))
                print(f"Errors of type {error_type}: {deduped_errors}")
                not_error[error_type] = self.window_gdf[~self.window_gdf[id_col].isin(deduped_errors)]

            for error_type, gdf in not_error.items():
                plot_to_html(gdf, title=f"Not_{error_type}")

            # Concatenatge and compute the delayed results
            arrays = [arr.compute() for arr in delayed_results]
            max_shape = max((arr.shape for arr in arrays), key=lambda x: (x[0], x[1]))
            print(f" Max Shape: {max_shape}")
            # Pad arrays to have the same shape, if necessary
            padded_arrays = []
            for arr in arrays:
                if arr.shape != max_shape:
                    # Calculate padding sizes
                    pad_width = []
                    for i in range(len(arr.shape)):
                        before_pad = (max_shape[i] - arr.shape[i]) // 2  # Padding before
                        after_pad = max_shape[i] - arr.shape[i] - before_pad  # Padding after
                        pad_width.append((before_pad, after_pad))
                    print(f"Padding: {pad_width}")
                    # Pad array and append to the list
                    padded_arr = da.pad(arr, pad_width, mode='constant',
                                        constant_values=-9999)  # Assuming -9999 is the nodata value
                    padded_arrays.append(padded_arr)
                    # plot_to_html(padded_arr, title=f"Padded_{arr.name}")
                else:
                    padded_arrays.append(arr)
                print(f"Array: \n{arr.where(arr != -9999)}")

            empty_dask_array = da.empty_like(self.target_da.data)
            concat_array = xr.DataArray(empty_dask_array, dims=self.target_da.dims, coords=self.target_da.coords)
            try:
                concat_array = da.block([arr for arr in padded_arrays])
            except ValueError as ve:
                print(f"Error during concatenation: {ve}")
                print(f"Shapes: {concat_array.shape}")
                pass

            # Wrap the concatenated Dask array in an xarray DataArray
            return xr.DataArray(concat_array, dims=self.target_dims, coords=self.target_da.coords
                                ).chunk(self.chunk_win).fillna(-9999)

    def compute_delays_asloop(self, delayed_results):
        # Compute the delayed results
        with tqdm(total=2 * len(delayed_results)) as pbar, ResourceProfiler(dt=1) as profiler:
            empty_dask_array = da.empty_like(self.target_da.data)
            # Wrap the dask array in a DataArray, copying over dimensions and coordinates
            concat_array = xr.DataArray(empty_dask_array, dims=self.target_da.dims, coords=self.target_da.coords)
            # Compute the delayed results
            for i, arr in enumerate(delayed_results):
                try:
                    arr = arr.compute().rename(f"a_{i}")
                    update_tqdm_pbar(pbar, desc_update=f'Array {i}: {arr}', number=1, append=False)
                except Exception as e:
                    print(f"Error during computation: {e}")
                    continue
                if isinstance(arr, xr.DataArray):
                    if len(arr.dims) == len(concat_array.dims) and arr.size:
                        concat_array = da.concatenate([concat_array, arr], axis=0)

                        # Rechunk after each batch to optimize memory usage
                        concat_array = concat_array.rechunk(self.chunk_win)

                        # Wrap the concatenated Dask array in an xarray DataArray
                        reprojected_da = xr.DataArray(concat_array, dims=self.target_dims, coords=self.target_da.coords)
                        update_tqdm_pbar(pbar, desc_update=f'Array {i}: {reprojected_da.name} finished', number=1,
                                         append=True)

        return reprojected_da


if __name__ == "__main__":
    import traceback

    source = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\source_01pct.tif"
    target = r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\test_WSE_0_2pct_DD.tif"
    s_arr = rioxr.open_rasterio(source)
    t_arr = rioxr.open_rasterio(target)
    print(f'Target CRS: {t_arr.rio.crs}')
    # Attempt to close any existing Dask client
    try:
        client = Client.current()
        client.close()
        print("Closed existing Dask client.")
    except ValueError:
        print("No existing Dask client found.")
    try:
        local_directory = os.path.join(os.getcwd(), "dask_temp")
        os.makedirs(local_directory, exist_ok=True)
        with LocalCluster(memory_limit='100GB', local_directory=local_directory) as cluster:
            client = Client(cluster)
            startup = DaskReproject(s_arr, t_arr)
            concatenated = startup.apply_reproject_match()
            concatenated.rio.to_raster(r"A:\Iowa_1A\02_mapping\Grids_Lower_Cedar\script_testing\reprojected_data.tif")
            client.close()
    except Exception as e:
        print(f"Error during Dask processing: {e}")
        raise traceback.print_exception(e)
    finally:
        cluster.close()
