# %%
import pandas as pd
import os
import time
import requests
from datetime import datetime
import json
import dask 

# defining parameters
start = 0
rows = 1000
end_limit = 350000
num_iterations = end_limit // rows

# build in retry attempts
# save to pickle first
# upload to S3, add code to do it automatically.

# output folder
output_base = "../data/data_gov_catalog"

# Create a folder named with the current ISO8601 timestamp
timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
run_folder = os.path.join(output_base, timestamp)
os.makedirs(run_folder, exist_ok=True)

# %%
# Non Dask Implementation:
import time

all_start = time.time()
results = []
for iteration in range(num_iterations):
    start_time = time.time()
    base_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    print(base_url)

    # Fetch the full dataset listing
    response = requests.get(base_url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch package list: {response.status_code} {response.text}")

    package_list = response.json().get('result', [])['results']
    file_name = f'{run_folder}/download_{start}_{start+rows}.json'

    with open(file_name, "w", encoding="utf-8") as file:
        json.dump(package_list, file, indent=4, ensure_ascii=False)

    package_list_df = pd.DataFrame.from_dict(package_list)
    # changing all var types to string in order to save it to parquet
    package_list_df = package_list_df.astype(str)
    file_name = f'{run_folder}/download_{start}_{start+rows}.parquet'
    package_list_df.to_parquet(file_name, index=False)
    end_time = time.time()
    print(f"time taken for rows {start} - {start+rows}: {end_time - start_time}")
    start += rows

    if iteration == 10:
        break

all_end = time.time()
print(all_end - all_start)

# %%
# Dask Implementation
# for loop to extract
# results = []

# @dask.delayed
# def delayed_function(base_url, start, rows, run_folder):
#     print(base_url)
#     # Fetch the full dataset listing
#     response = requests.get(base_url)

#     if response.status_code != 200:
#         raise Exception(f"Failed to fetch package list: {response.status_code} {response.text}")

#     package_list = response.json().get('result', [])['results']
#     file_name = f'{run_folder}/package_search_rows_{start}_{start+rows}.json'

#     with open(file_name, "w", encoding="utf-8") as file:
#         json.dump(package_list, file, indent=4, ensure_ascii=False)

#     start += rows

#     return None

# for iteration in range(num_iterations):
#     base_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
#     delayed_implementation = delayed_function(base_url, start, rows, run_folder)
#     start += rows
#     results.append(delayed_implementation)

# # %%
# detail_url = "https://catalog.data.gov/api/3/action/package_show"

# # %%
# # %%
# # Iterate through package IDs and fetch their details
# for package_id in package_list:

#     detail_response = requests.get(detail_url, params={"id": package_id})
#     if detail_response.status_code == 200:
#         package_data = detail_response.json()
#         output_path = os.path.join(run_folder, f"{package_id}.json")
#         with open(output_path, "w") as f:
#             f.write(detail_response.text)
#         break  # Successful fetch; exit retry loop
#     else:
#         raise Exception(f"Failed to fetch details for {package_id}: {detail_response.status_code} {detail_response.text}")
#             # if attempt < 4:
#             #     time.sleep((2 ** attempt))  # Exponential backoff
#             # else:
#             #     with open(os.path.join(run_folder, "errors.log"), "a") as err_log:
#             #         err_log.write(f"Error fetching {package_id}: {e}\n")

# # %%

# %%
