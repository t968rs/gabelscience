import time
import urllib.parse
import requests
from src.d00_utils.loggers import getalogger
from src.d01_processing.np_helpers import load_from_numpy


logger = getalogger("req_helpers", level=30)


def request(url, timeout=None):
    if timeout is None:
        timeout = 60

    req = requests.get(url)

    if req.status_code != 200:
        stime = time.time()
        while req.status_code != 200 or time.time() - stime < timeout:
            req = requests.get(url)
            if req.status_code == 200:
                break
            if time.time() - stime > timeout:
                logger.error(f"Request failed with status code {req.status_code}")
                logger.error(f"URL: {url}")
                return url

    return req.json()


def request_with_subset(query_url, feat_subset_path, query_params,
                        timeout=None, query_field="OBJECTID"):
    if timeout is None:
        timeout = 60

    # Load subset and convert to list
    to_get_subset = load_from_numpy(feat_subset_path).tolist()

    # Format list for map service query
    formatted_values = ", ".join([str(v) for v in to_get_subset])
    # f"{id_field} NOT IN ({formatted_values})"
    query_params['where'] = f"{query_field} IN ({formatted_values})"
    to_get_subset, formatted_values = None, None

    # Construct query url
    offset_query = urllib.parse.urlencode(query_params)
    final_url = f"{query_url}?{offset_query}"

    req = requests.get(final_url)

    if req.status_code != 200:
        stime = time.time()
        while req.status_code != 200 or time.time() - stime < timeout:
            req = requests.get(final_url)
            if req.status_code == 200:
                break
            if time.time() - stime > timeout:
                logger.error(f"Request failed with status code {req.status_code}")
                logger.error(f"URL: {final_url}")
                return final_url

    return req.json()


def fetch_json(url):
    try:
        response = requests.get(url, timeout=60)

        if response.status_code != 200:
            stime = time.time()
            while response.status_code != 200 or time.time() - stime < 300:
                req = requests.get(url)
                if req.status_code == 200:
                    break
                if time.time() - stime > 300:
                    logger.error(f"Request failed with status code {req.status_code}")
                    logger.error(f"URL: {url}")
                    return url
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return {}
