# %%
import boto3
from datetime import datetime, timezone
import json
import pandas as pd
import os
import requests
from requests.exceptions import RequestException
import time

# Connecting to AWS S3
# ADD THE FOLLOWING CREDENTIALS TO YOUR .ENV FILE
'''
You can create an access key here: https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/security_credentials?section=IAM_credentials
Or message Ben about it.
'''
aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]

# Initialize Boto3 client using environment variables
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

bucket_name = "govex-us-data-archive"

# %%
# defining parameters
start = 0           # Start index
rows = 1000         # Number of rows to fetch per request
end_limit = 310000  # Maximum number of rows to fetch
num_iterations = end_limit // rows  # Number of iterations
request_timeout = 60 # Timeout in seconds
max_retries = 5     # Maximum number of retries

# output folder
output_base = "data_gov_catalog_ndjson"
# Create a folder named with the current ISO8601 timestamp
timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
run_folder = os.path.join(output_base, timestamp)

# %% 
# Serial Implementation
import time

all_start = time.time()
results = []

# it's a function because it can happen in several places
def log_error_to_s3(url, error, start, rows):
    error_details = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"),
        "url": url,
        "error": str(error),
        "start": start,
        "rows": rows
    }
    error_file = f"{run_folder}/errors/error_{start:06d}_{start+rows:06d}.json"
    try:
        s3.put_object(
            Body=json.dumps(error_details, indent=4),
            Bucket=bucket_name,
            Key=f"Catalog/{error_file}"
        )
        print(f"🚨 Error log saved to S3: {error_file}")
    except Exception as e:
        print(f"❌ Failed to log error to S3: {e}")


while start < end_limit:
    start_time = time.time()
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    print(f"Fetching: {fetch_url}")

    # Retry logic
    success = False
    for attempt in range(max_retries):
        try:
            response = requests.get(fetch_url, timeout=request_timeout)
            response.raise_for_status()
            server_response = response.json()
            total_packages = server_response.get('result', {}).get('count', 0)
            package_list = server_response.get('result', {}).get('results', [])
            success = True
            break  # Exit retry loop on success

        except RequestException as e:
            print(f"⚠️ Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                retry_delay = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log_error_to_s3(fetch_url, e, start, rows)
                start += rows
                break

        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON: {e}")
            if attempt < max_retries - 1:
                print("Retrying...")
            else:
                log_error_to_s3(fetch_url, e, start, rows)
                start += rows
                break

    if success:
        if package_list:
            try:
                valid_lines = []
                error_lines = []

                for i, data in enumerate(package_list):
                    json_object = json.dumps(data)
                    check_length = len(json_object.splitlines())
                    
                    # checking for cases where we have invalid newline delimiters
                    if check_length == 1:
                        valid_lines.append(json_object)
                    else:
                        # logging errors out
                        print(f'Error in id = {data["id"]} at line number = {i}')
                        error_lines.append(json_object)

                # creating an ndjson object
                clean_ndjson_object = "\n".join(valid_lines)
                file_name = f'{run_folder}/download_{start:06d}_{start+rows:06d}.ndjson'

                s3.put_object(
                    Body=clean_ndjson_object,
                    Bucket=bucket_name,
                    Key=f"Catalog/{file_name}"
                )

                end_time = time.time()
                print(f"✅ Success: Rows {start} - {start+rows} of {total_packages} written to AWS: ({end_time - start_time:.2f} seconds)")
            except Exception as e:
                print(f"❌ Error saving rows {start} - {start+rows} of {total_packages} to S3: {e}")
        else:
            print("🟡 No data to save; skipping")
        start += rows


# %%
# done
print(f"✅ Completed: {time.time() - start_time:.2f} seconds")

## %% 
## For details
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
