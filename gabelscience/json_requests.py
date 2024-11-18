import os
import threading
import requests
from requests.adapters import HTTPAdapter, Retry
import json
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor as TPE
from concurrent.futures import as_completed
from references import online_links as ol


def get_api_info(base_url):
    params = {"$top": 1, "$skip": 500, '$count': "true"}  # Adjust "$top" for the maximum allowed per request
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raise an error for bad responses

    data = response.json()

    meta = data.get("metadata", {})
    other_keys = list(data.keys())[1:]
    print(f"Other keys: {other_keys}")
    single_entry = data.get(other_keys[0], [{}])[0]
    template_dict = {k: v for k, v in single_entry.items() if "layer" not in k.lower()}
    print(f"Template Dict: {template_dict}")

    return int(meta.get("count", 0)), other_keys, template_dict


def join_state_fips(df, join_field_name="STATE_FIPS"):
    state_df, states_list = ol.get_state_fips_df()
    df = df.join(state_df.set_index(join_field_name), on=join_field_name)
    return df

def list_to_df(data_list, sort_by=None):
    if sort_by:
        data_list.sort(key=lambda x: x[sort_by])
    return pd.DataFrame(data_list)


class UpdateFEMACommLayerInfo:
    renamer = {'communityIdNumber': 'CID', 'communityName': "COMM_NAME",
               "county": "CNTY_NAME", "state": "STATE_NAME", "stateCode": "STATE_FIPS",
               "countyCode": "CNTY_FIPS", "communityStatus": "COMM_STATUS", }

    def __init__(self, extra_params=None):
        self.base_url = ol.fema_communities_layer
        self.info = get_api_info(self.base_url)
        self.records = []
        self.rel_outpath = "../references/CID"
        self.extra_params = extra_params or {}

    def get_json_records(self, params):
        params.update({"$top": 1000, "$skip": 0})  # Adjust "$top" for the maximum allowed per request
        for key, value in params.items():
            print(f"\t{key}: {value}")

        # Get Count
        total_count, other_keys, template_dict = get_api_info(self.base_url)

        # print(f"Total records: {total_count}")

        def get_records(stop):
            local_params = params.copy()
            local_params.update({"$skip": stop})
            response = requests.get(self.base_url, params=local_params)
            response.raise_for_status()  # Raise an error for bad responses

            data = response.json()

            keys = list(data.keys())

            unfiltered = data.get(other_keys[0], [])
            recs = [{k: str(v) for k, v in rec.items() if k in template_dict} for rec in unfiltered]

            return recs

        # Page records
        stops = list(range(0, total_count, params["$top"]))
        print(f"Total stops: {len(stops)}, \n{stops}")
        with TPE(min([len(stops), 75])) as executor:
            print(f"Workers: {executor._max_workers}", end="\n\n")
            with tqdm(total=total_count, desc="Fetching Records") as pbar:
                for batch in executor.map(get_records, stops):
                    # print(f"Batch: {batch}")
                    self.records.extend(batch)
                    pbar.update(len(batch))

    def update_fema_communites(self):
        self.get_json_records(self.extra_params)
        print("Total records fetched:", len(self.records))
        df = pd.DataFrame(self.records).rename(columns=self.renamer)
        df = df.dropna(subset=["CID"])
        df = df.astype(str)
        df = df.drop_duplicates(subset=["CID"])

        return df

    def update_community_excel(self):
        comm_df = self.update_fema_communites()
        comm_df = join_state_fips(comm_df)
        print(f"--Columns: \n{comm_df.columns}")
        outpath = f"{self.rel_outpath}/fema_nfip_communities.xlsx"
        comm_df.to_excel(outpath, index=False, sheet_name="NFIP_Communities")


class CreatestatenfhlLinks:
    def __init__(self, extra_params=None):
        self.base_url = ol.msc_search
        # self.info = get_api_info(self.base_url)
        self.cid_excel = "../references/CID/fema_nfip_communities.xlsx"
        self.records = []
        self.rel_outpath = "../references/NFHL"
        self.scratch_dir = "../references/NFHL/json_scratch"
        os.makedirs(self.rel_outpath, exist_ok=True)
        self.extra_params = extra_params or {}
        self.state_sampeld_cid_data = None
        self.dtypes = {"CID": str, "STATE_FIPS": str, "CNTY_FIPS": str}
        self.state_info_df, _ = ol.get_state_fips_df()
        print(f"State Info: \n{self.state_info_df.head()}")
        self.output_file = f"{self.rel_outpath}/state_nfhl_links.xlsx"
        self.getted_urls = []

    def __repr__(self):
        return f"CreatestatenfhlLinks({self.cid_excel})"

    @property
    def output_columns(self):
        return {"searchSource": None,
        "ref_STATE_ABBREV_NAME": "STATE_ABBREV",
        "ref_PRODUCT_TYPE": "PRODUCT_TYPE",
        "product_ID": "product_ID",
        "product_TYPE_ID": "TYPE_ID",
        "product_SUBTYPE_ID": "SUBTYPE_ID",
        "product_NAME": "product_NAME",
        "product_DESCRIPTION": "DESCRIPTION",
        "product_EFFECTIVE_DATE": "EFFECTIVE_DATE",
        "product_ISSUE_DATE": "ISSUE_DATE",
        "product_EFFECTIVE_DATE_STRING": "EFFECTIVE_DATE_STRING",
        "product_POSTING_DATE": "POSTING_DATE",
        "product_EFFECTIVE_FLAG": None,
        "product_FILE_PATH": "FILE_PATH",
        "product_FILE_SIZE": "FILE_SIZE",
                      "STATE_FIPS": "STATE_FIPS",
                      "STATE": "STATE",
                      "STUSAB": "STUSAB",
                      "STATENS": "STATENS",
                      "STATE_NAME": "STATE_NAME"}

    @property
    def base_dict(self):
        return {
        "utf8": "✓",
        "affiliate": "fema",
        "query": "",
        "selstate": "",
        "selcounty": "",
        "selcommunity": "",
        "jurisdictionkey": "",
        "jurisdictionvalue": "",
        "searchedCid": "",  # Same as selcommunity
        "searchedDateStart": "",
        "searchedDateEnd": "",
        "txtstartdate": "",
        "txtenddate": "",
        "method": "search"
    }

    def sample_cid_data(self):
        print("Sampling CID data...")
        print(f"Reading: {self.cid_excel}")
        cid_df = pd.read_excel(self.cid_excel, dtype=self.dtypes)
        sampled_cid_data = cid_df.groupby('STATE_FIPS', group_keys=False).apply(
            lambda x: x.sample(1)
        ).reset_index(drop=True)
        return sampled_cid_data

    def construct_params(self, state_fips):
        state_params = self.base_dict.copy()

        state_data = self.state_sampeld_cid_data.loc[self.state_sampeld_cid_data["STATE_FIPS"] == state_fips]
        # print(f'\tState Data: \n{state_data}')
        if state_data.empty:
            raise ValueError(f"No data for state: {state_fips}")
        state_data = state_data.iloc[0]

        state_params.update({
            "selstate": state_data["STATE_FIPS"],
            "selcounty": state_data["CNTY_FIPS"],
            "selcommunity": state_data["CID"],
            "searchedCid": state_data["CID"]
        })

        return state_params

    def get_json_records(self):
        state_links = []

        pbar = tqdm(total=len(self.state_sampeld_cid_data), desc="Fetching Statewide Links")
        for idx, row in self.state_sampeld_cid_data.iterrows():
            state_params = self.construct_params(row["STATE_FIPS"])
            # print(f"State Params: {state_params}")
            state_fips = row["STATE_FIPS"]

            response = requests.get(self.base_url, params=state_params)
            response.raise_for_status()
            self.getted_urls.append(response.url)
            if response.status_code == 200:
                data = response.json()
                effective = data.get("EFFECTIVE")
                with open(f"{self.scratch_dir}/{state_fips}.json", "w") as f:
                    effective['link'] = response.url
                    json.dump(effective, f, indent=4)

                state_info = effective.get("NFHL_STATE_DATA", [])
                if not state_info:
                    print(f"No data for state: {state_fips}")
                    continue
                state_info = [{**rec, "STATE_FIPS": state_fips} for rec in state_info]
                self.records.extend(state_info)

                product_id = state_info[0].get("product_NAME")
                if product_id:
                    download_link = f"https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&productID={product_id}"
                    state_links.append({"STATE_FIPS": state_fips, "Download Link": download_link})
            pbar.update(1)

        return state_links

    def dataframe_post_processing(self, df):
        df = df.dropna(subset=["STATE_FIPS"])
        df = df.dropna(axis=1, how="all")
        for col in df.columns:
            if col.endswith("DATE"):
                print(f'\tConverting: {col}')
                df[col] = pd.to_datetime(df[col], unit='ms')
                df[col] = df[col].dt.strftime('%Y/%m/%d')
                print(f'\t{df[col].unique().tolist()[:2]}')
        df = df.join(self.state_info_df.set_index("STATE_FIPS"), on="STATE_FIPS")
        return df

    def update_state_nfhl_links(self):
        self.state_sampeld_cid_data = self.sample_cid_data()
        print(f'Sampled CID Data: \n{self.state_sampeld_cid_data.head()}')
        state_links = self.get_json_records()
        state_links = pd.DataFrame(state_links)

        print("Total records fetched:", len(self.records))
        link_df = pd.DataFrame(self.records)
        link_df = self.dataframe_post_processing(link_df)

        print(link_df.head())
        print(f"Writing to: {self.output_file}")
        with pd.ExcelWriter(self.output_file, engine='openpyxl', mode='a', if_sheet_exists="replace") as writer:
            link_df.to_excel(writer, index=False, sheet_name="State_NFHL_Info")
            state_links.to_excel(writer, index=False, sheet_name="Statewide_NFHL_Links")

        return self.output_file


class DownloadLinksFromDataFrame:
    def __init__(self, excel_file):
        self.links_df = pd.read_excel(excel_file, sheet_name="Statewide_NFHL_Links")
        self.state_info_df = pd.read_excel(excel_file, sheet_name="State_NFHL_Info")
        self.dtypes = {"STATE_FIPS": str}
        self.scratch_dir = "../references/NFHL/json_scratch"
        self.rel_outpath = "../references/NFHL"
        os.makedirs(self.rel_outpath, exist_ok=True)

    def download_links_parallel(self, max_workers=5):
        """
        Parallelizes downloading links using a thread pool.

        Args:
            max_workers (int): Maximum number of threads to use.
        """
        results = []
        pbar = tqdm(total=len(self.links_df), desc="Downloading Links")
        with requests.Session() as session:
            retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
            session.mount("https://", HTTPAdapter(max_retries=retries))
            with TPE(max_workers=max_workers) as executor:
                # Submit download tasks for each row
                futures = [
                    executor.submit(self.download_a_link, row=row, session=session)
                    for _, row in self.links_df.iterrows()
                ]

                # Collect results
                for future in as_completed(futures):
                    results.append(future.result())
                    pbar.update(1)

        print(f"Downloaded {len(results)} files successfully.")
        return results

    def download_a_link(self, row, session):

        state_fips = row["STATE_FIPS"]
        state_abbrev = self.state_info_df.loc[self.state_info_df["STATE_FIPS"] == state_fips, "STUSAB"].iloc[0]

        download_link = row["Download Link"]
        with threading.Lock():
            product_date = self.state_info_df.loc[self.state_info_df["STATE_FIPS"] ==
                                                  state_fips]["product_POSTING_DATE"].iloc[0]

        with session.get(download_link, stream=True) as response:
            response.raise_for_status()
            with open(f"{self.rel_outpath}/NFHL_{state_abbrev}.zip", "wb") as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
        return {"STATE_FIPS": state_fips, "STATE_ABBREV": state_abbrev,
                "Download Link": download_link, "Product Date": product_date}


if __name__ == "__main__":
    # links = CreatestatenfhlLinks()
    # outpath = links.update_state_nfhl_links()
    # print(outpath)
    outpath = r"E:\automation\toolboxes\gabelscience\references\NFHL\state_nfhl_links.xlsx"

    downloader = DownloadLinksFromDataFrame(outpath)
    results_info = downloader.download_links_parallel()

    results_table = pd.DataFrame(results_info)
    results_table.to_excel(f"{downloader.rel_outpath}/download_results.xlsx", index=False)

# https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&productID=NFHL_01_20241004
# https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&productID=265597950
# https://msc.fema.gov/portal/downloadProduct?productTypeID=NFHL&productSubTypeID=NFHL_STATE_DATA&product_FILE_PATH=NFHL_01_20241004.zip





