# %%
import pandas as pd
import os
import time
import requests
from datetime import datetime
import json
import boto3
from requests.exceptions import RequestException
import dask
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
end_limit = 305000
num_iterations = end_limit // rows

max_retries = 4
retry_delay = 2  # Wait time in seconds between retries


# build in retry attempts
# save to pickle first; flatten out 
# upload to S3, add code to do it automatically.

# output folder
output_base = "data_gov_catalog"
# Create a folder named with the current ISO8601 timestamp
timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
run_folder = os.path.join(output_base, timestamp)

# Function to fetch and upload data (with retries)
@dask.delayed
def fetch_and_upload_data(start, rows, max_retries, retry_delay):
    base_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    success = False
    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, timeout=10)
            response.raise_for_status()  # Check for HTTP errors

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

            print(f"✅ Success: Rows {start} - {start+rows}")
            success = True
            return True  # Return success flag

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

                return False  # Return failure flag

# Prepare a list of tasks for Dask
tasks = [fetch_and_upload_data(start + i * rows, rows, max_retries, retry_delay)
         for i in range(num_iterations)]

dask.compute(tasks)