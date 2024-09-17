from src.d00_utils import files_finder
from gabelscience import mask_rasters

root_folder = r"A:\Iowa_2A\02_mapping\Grids_SouthSkunk\01b_Splits\WSE_02pct"
terrain_path = r"A:\Iowa_2A\02_mapping\Grids_SouthSkunk\terrain_DEM.tif"
masking_raster_path = r"A:\Iowa_2A\02_mapping\Grids_SouthSkunk\04_DRAFT_DFIRM\intmd_DRAFT_DFIRM_WSE_01pct\mask_test.tif"

raster_paths = files_finder.RasterFinder().get_raster_list(root_folder)

for path in raster_paths:
    init = mask_rasters.MaskIt(masking_raster_path, epsg_code=3417, raster_value=0.0001, rawraster=path,
                               gridtype="wse depth", terrain=terrain_path, batch_script=True)
    init.run_scenarios()
