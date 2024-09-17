import requests
from bs4 import BeautifulSoup
import time

def simulate_typing(session, base_url, search_term):
    search_url = f'{base_url}/liveSearch'
    params = {
        'suggest': 'productid',
        'term': search_term
    }
    response = session.get(search_url, params=params)
    response.raise_for_status()
    return response.json()  # Assuming the response is JSON

def select_search_result(results, desired_result):
    for result in results:
        if desired_result in result['label']:
            return result['value']
    return None

def submit_search_form(session, base_url, selected_value):
    search_url = f'{base_url}/advanceSearch#searchresultsanchor'
    params = {
        'productid': selected_value
    }
    response = session.get(search_url, params=params)
    response.raise_for_status()
    return response.text

def parse_search_results(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {}
    table = soup.find('table', {'id': 'searchResultsTable'})
    if table:
        for row in table.find_all('tr')[1:]:  # Skip header row
            cells = row.find_all('td')
            state_fips = cells[0].text.strip()  # Adjust index based on actual table structure
            download_link = cells[-1].find('a')['href']  # Assuming the download link is in the last cell
            data[state_fips] = download_link
    return data

def download_file(url, save_path):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    with open(save_path, 'wb') as file:
        file.write(response.content)


import os
states = []
for s in states:
    base_url = 'https://msc.fema.gov/portal'
    search_term = 'desired_jurisdiction_name'  # Replace with actual search term
    desired_result = 'desired_FEMA_ID'  # Replace with actual desired result

    with requests.Session() as session:
        # Step 1: Simulate typing
        results = simulate_typing(session, base_url, search_term)

        # Step 2: Select the desired result
        selected_value = select_search_result(results, desired_result)
        if not selected_value:
            print(f'No matching result found for {desired_result}')
        else:
            # Step 3: Submit the search form
            html_content = submit_search_form(session, base_url, selected_value)

            # Step 4: Parse the search results
            state_download_links = parse_search_results(html_content)

            # Directory to save downloaded files
            download_dir = '../test_data/downloads'
            os.makedirs(download_dir, exist_ok=True)

            # Download files for each state
            for state_fips, download_link in state_download_links.items():
                save_path = os.path.join(download_dir, f'{state_fips}.zip')
                try:
                    download_file(download_link, save_path)
                    print(f'Successfully downloaded: {save_path}')
                except requests.exceptions.RequestException as e:
                    print(f'Failed to download {download_link}: {e}')