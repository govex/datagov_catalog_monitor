import os
import time
import requests
from datetime import datetime

def fetch_data():
    base_url = "https://catalog.data.gov/api/3/action/package_list"
    detail_url = "https://catalog.data.gov/api/3/action/package_show"
    output_base = "data_gov_catalog"

    # Create a folder named with the current ISO8601 timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    run_folder = os.path.join(output_base, timestamp)
    os.makedirs(run_folder, exist_ok=True)

    # Fetch the full dataset listing
    response = requests.get(base_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch package list: {response.status_code} {response.text}")

    package_list = response.json().get('result', [])

    # Iterate through package IDs and fetch their details
    for package_id in package_list:
        for attempt in range(5):  # Up to 5 attempts
            try:
                detail_response = requests.get(detail_url, params={"id": package_id})
                if detail_response.status_code == 200:
                    package_data = detail_response.json()
                    output_path = os.path.join(run_folder, f"{package_id}.json")
                    with open(output_path, "w") as f:
                        f.write(detail_response.text)
                    break  # Successful fetch; exit retry loop
                else:
                    raise Exception(f"Failed to fetch details for {package_id}: {detail_response.status_code} {detail_response.text}")
            except Exception as e:
                if attempt < 4:
                    time.sleep((2 ** attempt))  # Exponential backoff
                else:
                    with open(os.path.join(run_folder, "errors.log"), "a") as err_log:
                        err_log.write(f"Error fetching {package_id}: {e}\n")

if __name__ == "__main__":
    fetch_data()
