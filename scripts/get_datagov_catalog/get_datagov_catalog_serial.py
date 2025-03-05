# %%
# imports and initialization
import boto3
from datetime import datetime, timezone
import json
import pandas as pd
import os
import requests
from requests.exceptions import RequestException
import time
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

all_start = time.time()

# Connecting to AWS S3
# ADD THE FOLLOWING CREDENTIALS TO YOUR .ENV FILE
'''
You can create an access key here: https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/security_credentials?section=IAM_credentials
'''
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
aws_region = os.environ.get("AWS_REGION", None)

# Get the cache directory from environment variable
cache_dir = Path(os.environ['DATA_CACHE_DIR'])

# crash the script if the credentials are not found
if aws_access_key is None or aws_secret_key is None or aws_region is None:
    raise ValueError("AWS credentials or region not found in environment variables")

# crash the script if the cache directory is not found
if cache_dir is None:
    raise ValueError("Data cache directory not found in environment variables")

# Make sure the cache directory exists
cache_dir.mkdir(exist_ok=True)

# Create timestamp subfolder
timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
timestamp_dir = cache_dir / timestamp
timestamp_dir.mkdir(exist_ok=True)


# Initialize Boto3 client using environment variables
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

bucket_name = "govex-us-data-archive"

start = 0           # Start index
rows = 1000         # Number of rows to fetch per request
request_timeout = 60 # Timeout in seconds
max_retries = 5     # Maximum number of retries

# output folder
output_base = "data_gov_catalog_ndjson"
# Create a folder named with the current ISO8601 timestamp
timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
run_folder = os.path.join(output_base, timestamp)

results = []

# %%
# function definitions

# define a function to get the count of records
def get_count_of_records():
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start=0&rows=0"
    response = requests.get(fetch_url, timeout=request_timeout)
    response.raise_for_status()
    server_response = response.json()
    record_count = server_response.get('result', {}).get('count', 0)
    logger.info(f"📊 Records found: {record_count}")
    return record_count

# define a function to get the package list, with fallback logic
def get_package_list(start, rows, timeout=request_timeout, retries=max_retries, backoff=2):
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    logger.info(f"🔍 Fetching: {fetch_url}")
    
    for attempt in range(retries):
        try:
            response = requests.get(fetch_url, timeout=timeout)
            response.raise_for_status()
            server_response = response.json()
            return server_response.get('result', {}).get('results', [])
            
        except (RequestException, json.JSONDecodeError) as e:
            logger.warning(f"⚠️ Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                retry_delay = backoff ** attempt  # Exponential backoff
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ All {retries} attempts failed")
                log_error_to_s3(fetch_url, e, start, rows)
                return []

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
        logger.warning(f"🚨 Error log saved to S3: {error_file}")
    except Exception as e:
        logger.error(f"❌ Failed to log error to S3: {e}")

# %%
# get the data

# run the loop until we have fetched all records, plus a buffer, as the catalog can change while we're running
end = get_count_of_records() + 5000
while start < end:
    start_time = time.time()
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    logger.info(f"🔍 Fetching: {fetch_url}")

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

        except (RequestException, json.JSONDecodeError) as e:
            logger.warning(f"⚠️ Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                retry_delay = 2 ** attempt  # Exponential backoff
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
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
                        # error if we have more than 1 new line breaks in an object
                        logger.error(f'❌ Error in id = {data["id"]} at line number = {i}')
                        error_lines.append(json_object)

                # creating an ndjson object
                clean_ndjson_object = "\n".join(valid_lines)
                file_name = f'{run_folder}/download_{start:06d}_{start+rows:06d}.ndjson'

                s3.put_object(
                    Body=clean_ndjson_object,
                    Bucket=bucket_name,
                    Key=f"Catalog/{file_name}"
                )

                # also save the data to the local cache directory
                local_file = timestamp_dir / f'download_{start:06d}_{start+rows:06d}.ndjson'
                local_file.write_text(clean_ndjson_object)

                end_time = time.time()
                logger.info(f"✅ Success: Rows {start} - {start+rows} of {total_packages} written to AWS: ({end_time - start_time:.2f} seconds)")
            except Exception as e:
                logger.error(f"❌ Error saving rows {start} - {start+rows} of {total_packages} to S3: {e}")
        else:
            logger.warning("🟡 No data to save; skipping")
        start += rows


# %%
# done
logger.info(f"✅ Completed: {time.time() - all_start:.2f} seconds")

