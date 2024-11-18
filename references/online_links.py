import geopandas as gpd

# NFHL Online

EFFECTIVE_NFHL_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"

INFO_SUFFIX = "info/iteminfo?f=pjson"

NFHL_IDS = {0: 'NFHL Availability',
 1: 'LOMRs',
 3: 'FIRM Panels',
 4: 'Base Index',
 5: 'PLSS',
 6: 'Topographic Low Confidence Areas',
 7: 'River Mile Markers',
 8: 'Datum Conversion Points',
 9: 'Coastal Gages',
 10: 'Gages',
 11: 'Nodes',
 12: 'High Water Marks',
 13: 'Station Start Points',
 14: 'Cross-Sections',
 15: 'Coastal Transects',
 16: 'Base Flood Elevations',
 17: 'Profile Baselines',
 18: 'Transect Baselines',
 19: 'Limit of Moderate Wave Action',
 20: 'Water Lines',
 22: 'Political Jurisdictions',
 23: 'Levees',
 24: 'General Structures',
 25: 'Primary Frontal Dunes',
 26: 'Hydrologic Reaches',
 27: 'Flood Hazard Boundaries',
 28: 'Flood Hazard Zones',
 29: 'Seclusion Boundaries',
 30: 'Alluvial Fans',
 31: 'Subbasins',
 32: 'Water Areas',
 34: 'LOMAs'}

NFHL_FC_NAMES = {
    "0": "NFHL_Availability",
    "1": "S_LOMR",
    "3": "S_FIRM_PAN",
    "4": "S_BASE_INDEX",
    "5": "S_PLSS_AR",
    "6": "S_Topo_Low_Conf_Areas",
    "7": "S_RIV_MRK",
    "8": "S_DATUM_CONV_PT",
    "9": "S_Coastal_Gages",
    "10": "S_GAGE",
    "11": "S_NODES",
    "12": "S_HWM",
    "13": "S_STN_START",
    "14": "S_XS",
    "15": "S_Coastal_Transects",
    "16": "S_BFE",
    "17": "S_PROFIL_BASLN",
    "18": "S_Transect_Baselines",
    "19": "S_Limit_of_Moderate_Wave_Action",
    "20": "S_WTR_LN",
    "22": "S_POL_AR",
    "23": "S_LEVEE",
    "24": "S_GEN_STRUCT",
    "25": "S_Primary_Frontal_Dunes",
    "26": "S_HYDRO_REACH",
    "27": "S_FLD_HAZ_LN",
    "28": "S_FLD_HAZ_AR",
    "29": "S_Seclusion_Boundaries",
    "30": "S_Alluvial_Fans",
    "31": "S_SUBBASINS",
    "32": "S_WTR_AR",
    "34": "S_LOMAs"
}

STATE_FIPS = "https://www2.census.gov/geo/docs/reference/state.txt"

fema_communities_layer = "https://www.fema.gov/api/open/v1/NfipCommunityLayerComprehensive"
msc_search = "https://msc.fema.gov/portal/advanceSearch"


def get_state_fips_df():
    state_fips_url = STATE_FIPS
    state_fips_df = gpd.pd.read_csv(state_fips_url, sep="|")
    state_fips_df.columns = [c.strip() for c in state_fips_df.columns]
    state_fips_df["STATE_FIPS"] = state_fips_df["STATE"].apply(lambda x: f"{x:02d}")
    state_fips_df['STATENS'] = state_fips_df['STATENS'].apply(lambda x: f"{x:011d}")
    state_fips_df['STATE_NAME'] = state_fips_df['STATE_NAME'].apply(lambda x: x.strip())
    state_fips_df['STUSAB'] = state_fips_df['STUSAB'].apply(lambda x: x.strip())

    return state_fips_df, state_fips_df["STATE"].unique().tolist()
