class FEMAtables:
    def __init__(self, table_type):
        self.table_type = table_type

    @property
    def s_bfe(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'BFE_LN_ID', 'ELEV',
            'LEN_UNIT', 'V_DATUM', 'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'R',
                 'R', 'R', 'R')
        f_types = ('Text', 'Text', 'Text', 'Double',
                   'Text', "Text", "Text")
        length_prec = (6, 11, 25, '',
                       16, 17, 11)
        domain_relate = ('', '', '', '',
                         'D_Length_Units', 'D_V_Datum', 'L_Source_Cit')

        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    def s_alluvial_fan(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'ALLUVL_ID', 'ACTIVE_FAN', 'FANAPEX_DA', 'AREA_UNITS', 'FANAPEX_Q', 'DISCH_UNIT',
            'FAN_VEL_MN', 'FAN_VEL_MX', 'VEL_UNIT', 'DEPTH', 'DEPTH_UNIT', 'FLD_ZONE', 'ZONE_SUBTY', 'METH_DESC',
            'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'R', 'R', 'R', 'R', 'R', 'A', 'A', 'A', 'A', 'A', 'R', 'A', 'A', 'R')
        f_types = ('Text', 'Text', 'Text', 'Text', 'Double', 'Text', 'Double', 'Text', 'Double', 'Double',
                   'Text', 'Double', 'Text', 'Text', 'Text', 'Text', 'Text')
        length_prec = (6, 11, 25, 1, None, 17, None, 3, None, None, 20, None, 16, 17, 72, 254, 11)
        domain_relate = (None, None, None, "D_TrueFalse", None, "D_Area_Units", None, "D_Discharge_Units", None, None,
                         "D_Velocity_Units", None, "D_Length_Units", "D_Zone", "D_Zone_Subtype", None, "L_Source_Cit")

        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @property
    def s_fld_haz_ar(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'FLD_AR_ID', 'STUDY_TYP', 'FLD_ZONE', 'ZONE_SUBTY', 'SFHA_TF', 'STATIC_BFE',
            'V_DATUM', 'DEPTH', 'LEN_UNIT', 'VELOCITY', 'VEL_UNIT', 'AR_REVERT', 'AR_SUBTRV', 'BFE_REVERT',
            'DEP_REVERT',
            'DUAL_ZONE', 'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'R', 'R', 'A', 'R', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'R')
        f_types = (
            'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Double', 'Text', 'Double', 'Text', 'Double',
            'Text',
            'Text', 'Text', 'Double', 'Double', 'Text', 'Text')
        length_prec = (6, 11, 25, 38, 17, 76, 1, '', 17, '', 16, '', 20, 17, 76, '', '', 1, 11)
        domain_relate = (
            '', '', '', 'D_Study_Typ', 'D_Zone', 'D_Zone_Subtype', 'D_TrueFalse', '', 'D_V_Datum', '', 'D_Length_Units',
            '',
            'D_Velocity_Units', 'D_Zone', 'D_Zone_Subtype', '', '', 'D_TrueFalse', 'L_Source_Cit')
        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @property
    def s_fld_haz_ln(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'FLD_LN_ID', 'LN_TYP', 'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'R', 'R')
        f_types = (
            'Text', 'Text', 'Text', 'Text', 'Text')
        length_prec = (6, 11, 25, 26, 11)
        domain_relate = (
            '', '', '', 'D_Ln_Typ', 'L_Source_Cit')

        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @property
    def s_gen_struct(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'STRUCT_ID', 'STRUCT_TYP',
            'CST_STRUCT', 'STRUCT_NM', 'WTR_NM', 'LOC_DESC',
            'STRUC_DESC', 'SHOWN_FIRM', 'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'R',
                 'A', 'A', 'R', 'A',
                 'A', 'R', 'R')
        f_types = ('Text', 'Text', 'Text', 'Text',
                   'Text', 'Text', 'Text', 'Text',
                   'Text', 'Text', 'Text')
        length_prec = (6, 11, 25, 64,
                       29, 50, 100, 254,
                       254, 1, 11)
        domain_relate = ('', '', 'L_Cst_Struct', 'D_Struct_Typ',
                         'D_Cst_Struct', '', '', '',
                         'D_TrueFalse', 'L_Source_Cit')
        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @property
    def s_nodes(self):

        f_atts = ("Field", "R / A", "Type", "Length / Precision", "Joined", "Spatial / Lookup", "Domains")
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'NODE_ID', 'NODE_TYP',
            'WTR_NM', 'NODE_DESC', 'MODEL_ID', 'SOURCE_CIT')
        f_req = ('R', 'R', 'R', 'A',
                 'R', 'R', 'R', 'R')
        f_types = ('Text', 'Text', 'Text', 'Text',
                   'Text', 'Text', 'Text', 'Text',
                   'Text', 'Text', 'Text')
        length_prec = (6, 11, 25, 16,
                       100, 100, 100, 11)
        domain_relate = ('', '', '', 'D_Node_Typ',
                         '', '', '', 'L_Source_Cit')
        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @property
    def s_profil_basln(self):
        f_names = (
            'DFIRM_ID', 'VERSION_ID', 'BASELN_ID', 'WTR_NM',
            'SEGMT_NAME', 'WATER_TYP', 'STUDY_TYP', 'SHOWN_FIRM',
            'R_ST_DESC', 'R_END_DESC', 'V_DATM_OFF', 'DATUM_UNIT',
            'FLD_PROB1', 'FLD_PROB2', 'FLD_PROB3', 'SPEC_CONS1',
            'SPEC_CONS2', 'START_ID', 'SOURCE_CIT')
        f_req = (
            'R', 'R', 'R', 'R',
            'A', 'R', 'R', 'R',
            'R', 'R', 'A', 'A',
            'A', 'A', 'A', 'A',
            'A', 'R', "R")
        f_types = (
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text', 'Text', 'Text',
            'Text', 'Text')
        length_prec = (
            6, 11, 25, 100,
            254, 38, 38, 1,
            254, 254, 6, 16,
            254, 254, 254, 254,
            254, 25, 11)
        domain_relate = (
            '', '', '', '',
            '', 'D_Prof_Basln_Typ', 'D_Study_Typ', 'D_TrueFalse',
            '', '', '', 'D_Length_Units',
            '', '', '', '',
            '', 'S_Stn_Start', 'L_Source_Cit')
        return {"field names": f_names, "required": f_req, "field types": f_types,
                "lengths": length_prec, "domains": domain_relate}

    @staticmethod
    def d_zone(return_period):

        if return_period == '1%':
            fld_zone = 'A'
            zone_subty = None
        elif return_period == '0.2%':
            fld_zone = 'X'
            zone_subty = '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
        else:
            fld_zone, zone_subty = None, None

        return fld_zone, zone_subty

    def get_fields(self):
        # Populate the list of dictionaries by iterating field names and the above tuple indices
        # {"field names": f_names, "required": f_req, "field types": f_types,
        #                 "lengths": length_prec, "domains": domain_relate}

        allfields = []
        table_name = self.table_type
        table_fields_info = getattr(self, table_name)
        for i, f_name in enumerate(table_fields_info['field names']):
            thisfield_dict = {'field_name': f_name,
                              'field_type': table_fields_info['field types'][i],
                              'Required': table_fields_info['required'][i]}
            if table_fields_info['field types'][i] == 'Text':
                thisfield_dict['field_length'] = table_fields_info['lengths'][i]
            else:
                thisfield_dict['field_length'] = ''
            if table_fields_info['lengths'][i] != '':
                if "D_" in table_fields_info['domains'][i]:
                    thisfield_dict['Domains'] = table_fields_info['domains'][i]
                elif "L_" in table_fields_info['domains'][i]:
                    thisfield_dict['Related Table'] = table_fields_info['domains'][i]
                else:
                    thisfield_dict['Domains'] = ''
                    thisfield_dict['Related Table'] = ''

            print(f'{f_name}\n  {thisfield_dict}')
            allfields.append(thisfield_dict)

        return tuple(allfields)

if __name__ == "__main__":
    table = "s_fld_haz_ar"
    init = FEMAtables(table)
    fnames = []
    for field in init.get_fields():
        fnames.append(field['field_name'])

    print(fnames)


