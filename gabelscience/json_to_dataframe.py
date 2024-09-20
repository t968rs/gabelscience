import os
import json
import pandas as pd


def excel_to_dataframe(excel_path, sheet_name):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    return df


def dict_to_dataframe(dict_list):
    columns_to_create = []
    list_of_unnested_dicts = []

    for entry in dict_list:
        the_keys = list(entry.keys())
        for k in the_keys:
            if isinstance(entry[k], dict):
                unnested_dict = {}
                for key, value in entry[k].items():
                    unnested_dict[key] = value
                    if key not in columns_to_create:
                        columns_to_create.append(key)
                list_of_unnested_dicts.append(unnested_dict)
            else:
                to_add = [key for key in the_keys if key not in columns_to_create]
                columns_to_create.extend(to_add)
    print(f"Columns to Create: {columns_to_create}")
    if not list_of_unnested_dicts:
        df = pd.DataFrame(dict_list, columns=columns_to_create)
    else:
        df = pd.DataFrame(list_of_unnested_dicts, columns=columns_to_create)
    print(df.head())
    return df


def json_to_list(json_path, key_to_get):
    with open(json_path) as f:
        data = json.load(f)
        data = data[key_to_get]
    return data


if __name__ == "__main__":
    json_file = r"E:\automation\downloader\ref_data\USGS_1m_Projects.json"
    states_excel_path = r"E:\CTP_Metadata\Area_1A_Purchase_Geographies_ADDS.xlsx"
    states_sheet = "State_FIPS_Refs"
    output_path = r"E:\automation\downloader\ref_data"

    list_of_dict = json_to_list(json_file, "features")
    table_df = dict_to_dataframe(list_of_dict)
    table_df.sort_values(by=table_df.columns[0], inplace=True)

    # Do some custom stuff
    states_table = excel_to_dataframe(states_excel_path, states_sheet)
    state_abbr = states_table["Postal"].tolist()

    # Filter by state
    for state in state_abbr:
        filtered = table_df.loc[table_df["project"].str.contains(f"{state}_")]
        projects = filtered["project"].tolist()
        print(projects)
        out_txt = os.path.join(output_path, f"{state}_projects.txt")
        text_to_write = "(" + "".join([f"'{p}', " for p in projects]) + ")"
        with open(out_txt, "w") as f:
            f.write(text_to_write)


    # https://index.nationalmap.gov/arcgis/rest/services/3DEPElevationIndex/MapServer/18/query
