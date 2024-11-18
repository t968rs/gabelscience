from src.d00_utils.gbounds import extent_excel_from_fc


def string_to_affine(s):
    from rasterio.transform import Affine
    parts = str(s).split(',')  # Assuming the string is comma-separated
    print(f'Parts: {parts}')
    if len(parts) != 6 or len(parts) != 9:
        return False, "String does not contain exactly six parts"

    try:
        coefficients = [float(part) for part in parts]
    except ValueError:
        return False, "One or more parts of the string could not be converted to float"

    return True, Affine(*coefficients)


if __name__ == "__main__":
    feature_class = r"E:\Iowa_00_Tracking\01_statewide\IA_Statewide_Meta\S_Submittal_Info_IA_BLE.shp"
    field_name = "HUC8"
    out_loc = r"E:\CTP_Metadata\IA_Statewide_BLE"

    outpaths = extent_excel_from_fc(feature_class, field_name, out_loc)
    print(f"Finished")
    for path in outpaths:
        print(f"Saved: {path}")


