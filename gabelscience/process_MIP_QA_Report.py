import bs4.element
import pandas as pd
from bs4 import BeautifulSoup

CHECK_TYPES = ["Topology", "Attribute", "Schema", "Spatial"]


def extract_MIP_Report_info(link):
    # Load the HTML content
    with open(link) as file:
        content = file.read()

    # Parse the HTML content
    soup = BeautifulSoup(content, 'html.parser')

    # Initialize a list to store the extracted data
    data = []

    content_within = soup.find_all('div', class_='content')
    for results in content_within:
        error_message, table_name, check_type = None, None, None
        for child in results.children:
            if child:
                # print(f"\nChild: {type(child)}")
                if isinstance(child, bs4.element.NavigableString):
                    continue
                elif isinstance(child, bs4.element.Tag):
                    content = child.find('h5')
                    if content:
                        if "In Table" in content.text:
                            table_content = content.text.split(':')[1].strip()
                            table_name = table_content.split()[0]
                            record_no = table_content.split("record")[0].split()[-1].strip()
                            print(f"TABLE: {table_name} - {record_no} records")
                        else:
                            error_content = content.text
                            check_type = error_content.split(":")[0].split()[1].strip()
                            if "Field" not in check_type:
                                error_message = error_content.split(":")[-1].strip()
                                print(f"ERROR {check_type}\n   {error_message}")
                            else:
                                check_type = None
                        for error_content in child.descendants:
                            if error_content:

                                if isinstance(error_content, bs4.element.NavigableString):
                                    continue
                                elif isinstance(error_content, bs4.element.Tag):
                                    records = error_content.find_all('h6', class_='recordId')
                                    if records:
                                        print(f"   RECORDS: {len(records)}")
                                        for record in records:
                                            record_id = record.text.split(":")[1].strip().split()[0]
                                            # print(f"      RECORD ID: {record_id}")
                                            if check_type:
                                                data.append({"Check Type": check_type, "Error Message": error_message,
                                                             "Table": table_name, "Record ID": record_id})

    return pd.DataFrame(data).drop_duplicates()


def summarize_df(df):
    # Group the DataFrame by "Check Type" and "Table"
    for table in df["Table"].unique():
        summary = df[df["Table"] == table].groupby(["Check Type", "Error Message"]).size().unstack(fill_value=0)

        # Add a "Total" column to the summary DataFrame
        summary["Total"] = summary.sum(axis=1)
        print(f"{table} Summary: \n{summary}")

    unique_fc = df["Table"].unique()
    for fc in unique_fc:
        records = df["Record ID"][df["Table"] == fc].to_list()
        records = list(set(records))
        print(f"\n{fc} Records: {records}")


if __name__ == "__main__":
    # Create a DataFrame from the extracted data
    url = "../test_data/downloads/FIRM DB QA Submission Report_LNM_2024_0913_1848.htm"
    df = extract_MIP_Report_info(url)
    df.to_excel("../test_data/downloads/MIP_Report_Info_CD_LNM_2024_0913_0333.xlsx", index=False)

    # Display the DataFrame
    print(df)
    summarize_df(df)
