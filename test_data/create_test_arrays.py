import os

import numpy as np
import xarray as xr


class CreateTestArray:
    def __init__(self, data_size=(4096, 4096), nodata_value=-9999, perc_nodata=0.4, epsg_code=4326):
        self.data_size = data_size
        self.nodata_value = nodata_value
        self.perc_nodata = perc_nodata
        self.epsg_code = epsg_code
        self.crs = f"EPSG:{epsg_code}"

        self.np_array = None
        self.xarray = None

    def create_np_arrays(self):
        # Step 1: Create random data arrays
        self.np_array = np.random.randint(low=1, high=100, size=self.data_size)
        print(self.np_array)
        print(self.np_array[0])
        print(self.np_array[1])

    def add_nodata(self):
        array_size = self.np_array.size
        array_shape = self.np_array.shape
        num_nodata_points = int(round(array_size * self.perc_nodata))  # Number of NoData points to introduce
        # Generate random indices for NoData points
        nodata_indices_y = np.random.randint(0, self.np_array.shape[0], num_nodata_points)
        nodata_indices_x = np.random.randint(0, self.np_array.shape[1], num_nodata_points)

        # Assign NoData value to the selected indices
        self.np_array[nodata_indices_y, nodata_indices_x] = self.nodata_value

    def create_xarray_from_np(self):
        # Step 3: Create xarray.DataArray objects
        x = y = np.arange(0, 4096)  # Spatial coordinates
        self.xarray = xr.DataArray(self.np_array, dims=("y", "x"), coords={"y": y, "x": x})
        self.xarray.rio.write_crs(self.crs, inplace=True)
        print(self.xarray.rio.crs)

    def get_test_array(self):
        self.create_np_arrays()
        self.add_nodata()
        self.create_xarray_from_np()
        return self.xarray


if __name__ == "__main__":
    # Create a test array
    folder = "./test_data"
    for d in ["source", "target"]:
        result = CreateTestArray()
        test_array = result.get_test_array()
        outpath = os.path.join(folder, f"{d}_data.nc")
        test_array.to_netcdf(outpath)
