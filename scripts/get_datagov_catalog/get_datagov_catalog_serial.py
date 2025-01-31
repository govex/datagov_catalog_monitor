# %%
import pandas as pd
import os
import time
import requests
from datetime import datetime
import json
import boto3
from requests.exceptions import RequestException
import boto3
import os

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
start = 0
rows = 1000
end_limit = 310000
num_iterations = end_limit // rows

max_retries = 4
retry_delay = 2  # Wait time in seconds between retries

# output folder
output_base = "data_gov_catalog"
# Create a folder named with the current ISO8601 timestamp
timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
run_folder = os.path.join(output_base, timestamp)

# %% Serial Implementation
import time

all_start = time.time()
results = []

for iteration in range(num_iterations):
    start_time = time.time()
    base_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    print(f"Fetching: {base_url}")

    # Retry logic
    success = False
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, timeout=30)

            # Check for HTTP errors
            response.raise_for_status()

            # Parse JSON response
            package_list = response.json().get('result', {}).get('results', [])

            # File name for successful data
            file_name = f'{run_folder}/download_{start:06d}_{start+rows:06d}.json'

            # Upload to S3 (Success)
            s3.put_object(
                Body=json.dumps(package_list),
                Bucket=bucket_name,
                Key=f"Catalog/{file_name}"
            )

            end_time = time.time()
            print(f"✅ Success: Rows {start} - {start+rows} in {end_time - start_time:.2f} seconds")
            success = True
            break  # Exit retry loop on success

        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("❌ Max retries reached. Logging error.")

                # Error details
                error_details = {
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "url": base_url,
                    "error": str(e)
                }

                # File name for error logs
                error_file = f"{run_folder}/errors/error_{start:06d}_{start+rows:06d}.json"

                # Upload error log to S3
                s3.put_object(
                    Body=json.dumps(error_details, indent=4),
                    Bucket=bucket_name,
                    Key=error_file
                )
                print(f"🚨 Error log saved to S3: {error_file}")

    start += rows

# %%
## %% For details
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
