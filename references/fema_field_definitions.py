FIELD_DEFINITIONS = {
    "S_BFE": {
        "field names": ['DFIRM_ID', 'VERSION_ID', 'BFE_LN_ID', 'ELEV', 'LEN_UNIT', 'V_DATUM', 'SOURCE_CIT'],
        "required": ['R', 'R', 'R', 'R', 'R', 'R', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Double', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, '', 16, 17, 11],
        "domains": ['', '', '', '', 'D_Length_Units', 'D_V_Datum', 'L_Source_Cit'],
        "unique_id": "BFE_LN_ID",
        "fc_type": "Polyline",
    },
    "S_ALLUVIAL_FAN": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'ALLUVL_ID', 'ACTIVE_FAN', 'FANAPEX_DA', 'AREA_UNITS', 'FANAPEX_Q', 'DISCH_UNIT',
            'FAN_VEL_MN', 'FAN_VEL_MX', 'VEL_UNIT', 'DEPTH', 'DEPTH_UNIT', 'FLD_ZONE', 'ZONE_SUBTY', 'METH_DESC',
            'SOURCE_CIT'
        ],
        "required": ['R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'A', 'A', 'A', 'A', 'A', 'R', 'A', 'A', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Text', 'Double', 'Text', 'Double', 'Text', 'Double', 'Double', 'Text',
                        'Double', 'Text', 'Text', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, 1, None, 17, None, 3, None, None, 20, None, 16, 17, 72, 254, 11],
        "domains": [None, None, None, "D_TrueFalse", None, "D_Area_Units", None, "D_Discharge_Units", None, None,
                    "D_Velocity_Units", None, "D_Length_Units", "D_Zone", "D_Zone_Subtype", None, "L_Source_Cit"],
        "unique_id": "ALLUVL_ID",
        "fc_type": "Polygon",
    },
    "S_FLD_HAZ_AR": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'FLD_AR_ID', 'STUDY_TYP', 'FLD_ZONE', 'ZONE_SUBTY', 'SFHA_TF', 'STATIC_BFE',
            'V_DATUM', 'DEPTH', 'LEN_UNIT', 'VELOCITY', 'VEL_UNIT', 'AR_REVERT', 'AR_SUBTRV', 'BFE_REVERT',
            'DEP_REVERT', 'DUAL_ZONE', 'SOURCE_CIT'
        ],
        "required": ['R', 'R', 'R', 'R', 'R', 'A', 'R', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'R'],
        "field types": [
            'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Double', 'Text', 'Double', 'Text', 'Double',
            'Text', 'Text', 'Text', 'Double', 'Double', 'Text', 'Text'
        ],
        "lengths": [6, 11, 25, 38, 17, 76, 1, '', 17, '', 16, '', 20, 17, 76, '', '', 1, 11],
        "domains": [
            '', '', '', 'D_Study_Typ', 'D_Zone', 'D_Zone_Subtype', 'D_TrueFalse', '', 'D_V_Datum', '',
            'D_Length_Units', '', 'D_Velocity_Units', 'D_Zone', 'D_Zone_Subtype', '', '', 'D_TrueFalse', 'L_Source_Cit'
        ],
        "unique_id": "FLD_AR_ID",
        "fc_type": "Polygon",
    },
    "S_FLD_HAZ_LN": {
        "field names": ['DFIRM_ID', 'VERSION_ID', 'FLD_LN_ID', 'LN_TYP', 'SOURCE_CIT'],
        "required": ['R', 'R', 'R', 'R', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, 26, 11],
        "domains": ['', '', '', 'D_Ln_Typ', 'L_Source_Cit'],
        "unique_id": "FLD_LN_ID",
        "fc_type": "Polyline",
    },
    "S_GEN_STRUCT": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'STRUCT_ID', 'STRUCT_TYP', 'CST_STRUCT', 'STRUCT_NM', 'WTR_NM', 'LOC_DESC',
            'STRUC_DESC', 'SHOWN_FIRM', 'SOURCE_CIT'
        ],
        "required": ['R', 'R', 'R', 'R', 'A', 'A', 'R', 'A', 'A', 'R', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, 64, 29, 50, 100, 254, 254, 1, 11],
        "domains": ['', '', 'L_Cst_Struct', 'D_Struct_Typ', 'D_Cst_Struct', '', '', '', 'D_TrueFalse', 'L_Source_Cit'],
        "unique_id": "STRUCT_ID",
        "fc_type": "Polyline",
    },
    "S_LEVEE": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'LEVEE_ID', 'FC_SYS_ID', 'LEVEE_NM', 'LEVEE_TYP', 'WTR_NM', 'BANK_LOC',
            'USACE_LEV', 'DISTRICT', 'PL84_99TF', 'CONST_DATE', 'DGN_FREQ', 'FREEBOARD', 'LEVEE_STAT', 'PAL_DATE',
            'LEV_AN_TYP', 'FC_SEG_ID', 'OWNER', 'LEN_UNIT', 'SOURCE_CIT'
        ],
        "required": ['R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'A', 'R', 'A', 'A', 'A', 'R', 'A', 'R', 'A', 'R', 'R', 'R'],
        "field types": [
            'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Date', 'Text',
            'Double', 'Text', 'Date', 'Text', 'Text', 'Text', 'Text', 'Text'
        ],
        "lengths": [6, 11, 25, 25, 100, 24, 100, 100, 1, 13, 1, "", 50, '', 24, '', 27, 25, 100, 16, 11],
        "domains": [
            '', '', '', '', '', 'D_Levee_Typ', '', '', 'D_TrueFalse', 'D_USACE_District', 'D_TrueFalse', 'Date', '',
            'Double', 'D_Levee_Status', 'Date', 'D_Levee_Analysis_Type', '', '', 'D_Length_Units', 'L_Source_Cit'
        ],
        "unique_id": "LEVEE_ID",
        "fc_type": "Polyline",
    },
    "S_NODES": {
        "field names": ['DFIRM_ID', 'VERSION_ID', 'NODE_ID', 'NODE_TYP', 'WTR_NM', 'NODE_DESC', 'MODEL_ID', 'SOURCE_CIT'],
        "required": ['R', 'R', 'R', 'A', 'R', 'R', 'R', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, 16, 100, 100, 100, 11],
        "domains": ['', '', '', 'D_Node_Typ', '', '', '', '', '', '', 'L_Source_Cit'],
        "unique_id": "NODE_ID",
        "fc_type": "Point",
    },
    "S_PROFIL_BASLN": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'BASELN_ID', 'WTR_NM', 'SEGMT_NAME', 'WATER_TYP', 'STUDY_TYP', 'SHOWN_FIRM',
            'R_ST_DESC', 'R_END_DESC', 'V_DATM_OFF', 'DATUM_UNIT', 'FLD_PROB1', 'FLD_PROB2', 'FLD_PROB3', 'SPEC_CONS1',
            'SPEC_CONS2', 'START_ID', 'SOURCE_CIT'
        ],
        "required": [
            'R', 'R', 'R', 'R', 'A', 'R', 'R', 'R', 'R', 'R', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'R', "R"
        ],
        "field types": ['Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text'],
        "lengths": [
            6, 11, 25, 100,
            254, 38, 38, 1,
            254, 254, 6, 16,
            254, 254, 254, 254,
            254, 25, 11],
        "domains": [
            '', '', '', '',
            '', 'D_Prof_Basln_Typ', 'D_Study_Typ', 'D_TrueFalse',
            '', '', '', 'D_Length_Units',
            '', '', '', '',
            '', 'S_Stn_Start', 'L_Source_Cit'
        ],
        "unique_id": "BASELN_ID",
        "fc_type": "Polyline",
    },
    "S_SUBMITTAL_INFO": {
        "field names": [
            'DFIRM_ID', 'VERSION_ID', 'SUBINFO_ID', 'CASE_NO', 'CASE_DESC', 'SUBMIT_BY', 'HUC8', 'METHOD_TYP',
            'COMP_DATE', 'TASK_TYP', 'HYDRO_MDL', 'HYDRA_MDL', 'CST_MDL_ID', 'TOPO_SRC', 'TOPO_V_ACC', 'TOPO_H_ACC',
            'EFF_DATE', 'CONTRCT_NO', 'SOURCE_CIT'
        ],
        "required": [
            'R', 'R', 'R', 'R', 'R', 'R', 'A', 'R', 'R', 'R', 'A', 'A', 'A', 'A', 'A', 'A', 'R', 'R', 'R'
        ],
        "field types": [
            'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Date', 'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Date', 'Text', 'Text'
        ],
        "lengths": [
            6, 11, 25, 13, 254, 100, 8, 28, 'Default', 21, 40, 83, 25, 254, 254, 254, 'Default', 50, 11
        ],
        "domains": [
            '', '', '', '', '', '', '', 'D_Study_Mth', '', 'D_Task_Typ', 'D_Hydro_Mdl', 'D_Hydra_Mdl', 'L_Cst_Model',
            '', '', '', '', '', 'L_Source_Cit'
        ],
        "unique_id": "SUBINFO_ID",
        "fc_type": "Polygon",
    },
    'S_WTR_LN': {
        "field names": ['DFIRM_ID', 'VERSION_ID', 'WTR_LN_ID', 'WTR_NM', 'SHOWN_FIRM', 'SHOWN_INDX', 'SOURCE_CIT'],
        "required": ['R', 'R', 'R', 'R', 'A', 'A', 'R'],
        "field types": ['Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text'],
        "lengths": [6, 11, 25, 100, 1, 1, 11],
        "domains": ['', '', '', '', 'D_TrueFalse', 'D_TrueFalse', 'L_Source_Cit'],
        "unique_id": "WTR_LN_ID",
        "fc_type": "Polyline",
    }

}


def convert_fields_to_postgres_types(input_fema_table):
    from sqlalchemy import String, Integer, Float, Date, DOUBLE_PRECISION

    input_fema_table = input_fema_table.upper()
    print(f"Input Table: {input_fema_table}")
    table_info = FIELD_DEFINITIONS.get(input_fema_table)
    # print(f"Table Info: {table_info}")

    field_names = FIELD_DEFINITIONS.get(input_fema_table).get("field names")
    ftypes = FIELD_DEFINITIONS.get(input_fema_table).get("field types")

    field_types = {
        "Text": String,
        "Double": DOUBLE_PRECISION,
        "Date": Date,
        "Integer": Integer,
    }

    sql_types = {}
    for field in field_names:
        fname_idx = field_names.index(field)
        field_type = ftypes[fname_idx]
        sql_types[field] = field_types[field_type]

    return sql_types


