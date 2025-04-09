# %%
# imports and initialization
import boto3
from datetime import datetime, timezone
import json
import logging
import pandas as pd
from pathlib import Path
import os
import requests
from requests.exceptions import RequestException
import time

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
# Validate all required environment variables are present
required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "DATA_CACHE_DIR"]
missing_vars = [var for var in required_env_vars if var not in os.environ]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Get environment variables now that we know they exist
aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]
cache_dir = Path(os.environ["DATA_CACHE_DIR"]) / 'data_gov_catalog_ndjson'

# Make sure the cache directory exists
cache_dir.mkdir(parents=True, exist_ok=True)

# Create timestamp for both local and S3 paths
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
s3_base_prefix = "Catalog/data_gov_catalog_ndjson"  # Base S3 prefix

start = 0           # Start index
rows = 100         # Number of rows to fetch per request
request_timeout = 60 # Timeout in seconds
max_retries = 5     # Maximum number of retries

results = []

# %%
# function definitions

# define a function to get the count of records
def get_count_of_records():
    """Query the Data.gov API to get the total number of records available.
    
    Makes a single request to the package_search endpoint with rows=0 to get just the count.
    
    Returns:
        int: Total number of records available in the catalog
        
    Raises:
        RequestException: If there's a network or API error
        json.JSONDecodeError: If the response isn't valid JSON
    """
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start=0&rows=0"
    response = requests.get(fetch_url, timeout=request_timeout)
    response.raise_for_status()
    server_response = response.json()
    record_count = server_response.get('result', {}).get('count', 0)
    logger.info(f"📊 Records found: {record_count}")
    return record_count

# define a function to get the package list, with fallback logic
def get_catalog_records(start, rows, timeout=request_timeout, retries=max_retries, backoff=2):
    """Fetch a range of package records from the Data.gov API with retry logic.
    
    Args:
        start (int): Starting index for the range of records to fetch
        rows (int): Number of records to fetch
        timeout (int, optional): Request timeout in seconds. Defaults to request_timeout.
        retries (int, optional): Maximum number of retry attempts. Defaults to max_retries.
        backoff (int, optional): Base for exponential backoff between retries. Defaults to 2.
        
    Returns:
        list | None: List of package records if successful, None if all retries failed
              Empty list means successful request but no records in that range
              
    Note:
        The function implements exponential backoff for retries.
        For each retry attempt n, it waits backoff^n seconds before retrying.
    """
    fetch_url = f"https://catalog.data.gov/api/3/action/package_search?start={start}&rows={rows}"
    logger.info(f"🔍 Fetching: {fetch_url}")
    
    for attempt in range(retries):
        try:
            response = requests.get(fetch_url, timeout=timeout)
            response.raise_for_status()  # Will raise an exception if HTTP status indicates error
            
            server_response = response.json()
            if not server_response.get('success', False):
                # API indicates failure even though HTTP status was 200
                logger.error(f"❌ API returned application error for URL: {fetch_url}")
                if attempt < retries - 1:
                    retry_delay = backoff ** attempt
                    logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                return None
                
            # Both HTTP and API level success
            return server_response.get('result', {}).get('results', [])
            
        except requests.exceptions.HTTPError as e:
            # HTTP status code error
            logger.warning(f"⚠️ HTTP error (status {response.status_code}) for URL {fetch_url}: {e}")
            if attempt < retries - 1:
                retry_delay = backoff ** attempt
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ All {retries} HTTP attempts failed for URL {fetch_url}")
                return None
                
        except (RequestException, json.JSONDecodeError) as e:
            # Other request errors (timeout, connection, invalid JSON, etc)
            logger.warning(f"⚠️ Request failed for URL {fetch_url}: {e}")
            if attempt < retries - 1:
                retry_delay = backoff ** attempt
                logger.info(f"🔄 Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ All {retries} attempts failed for URL {fetch_url}: {e}")
                return None

def process_and_save_data(package_list, start, rows, start_time=None):
    """Process a list of package records and save them to both S3 and local cache.
    
    Processes the package records into NDJSON format, handling any records with
    invalid newlines. Saves the processed data to both S3 and the local cache directory.
    
    Args:
        package_list (list): List of package records to process
        start (int): Starting index of this batch of records
        rows (int): Number of records in this batch
        start_time (float, optional): Start time of processing for timing logs.
            If provided, logs will include processing duration.
            
    Returns:
        bool: True if processing and saving was successful, False if there were any errors
        
    Note:
        Files are saved with names in the format:
        - S3: {bucket_name}/{s3_base_prefix}/{timestamp}/download_{start:06d}_{start+rows:06d}.ndjson
        - Local: {timestamp_dir}/download_{start:06d}_{start+rows:06d}.ndjson
    """
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
        
        # Construct S3 key using consistent timestamp
        s3_key = f"{s3_base_prefix}/{timestamp}/download_{start:06d}_{start+rows:06d}.ndjson"
        
        # Save to S3
        s3.put_object(
            Body=clean_ndjson_object,
            Bucket=bucket_name,
            Key=s3_key
        )

        # Save to local cache
        local_file = timestamp_dir / f'download_{start:06d}_{start+rows:06d}.ndjson'
        local_file.write_text(clean_ndjson_object)

        if start_time:
            end_time = time.time()
            logger.info(f"✅ Success: Rows {start} - {start+rows} written to files ({end_time - start_time:.2f} seconds)")
        else:
            logger.info(f"✅ Success: Rows {start} - {start+rows} written to files")
            
        return True
    except Exception as e:
        logger.error(f"❌ Error processing rows {start} - {start+rows}: {e}")
        return False

# %%
# get the data

# Keep track of failed ranges to retry later
failed_ranges = []

# Get initial count
initial_count = get_count_of_records()
end = initial_count + 5000

# First pass: try to get all records
while start < end:
    start_time = time.time()
    package_list = get_catalog_records(start, rows)
    
    if package_list is None:
        # If we got an error, mark this range for retry
        failed_ranges.append((start, rows))
        logger.warning(f"🔄 Adding range {start}-{start+rows} to retry queue")
        start += rows
        continue
    elif not package_list:
        # If we got zero results but no error, log and continue
        logger.info(f"ℹ️ No results for range {start}-{start+rows}, moving on")
        start += rows
        continue

    if not process_and_save_data(package_list, start, rows, start_time):
        failed_ranges.append((start, rows))
    
    start += rows

# Retry failed ranges if the total count hasn't changed
unresolved_failures = []  # Track failures that couldn't be fixed
if failed_ranges:
    logger.info(f"🔄 Attempting to retry {len(failed_ranges)} failed ranges...")
    current_count = get_count_of_records()
    
    if current_count == initial_count:
        for retry_start, retry_rows in failed_ranges:
            logger.info(f"🔄 Retrying range {retry_start}-{retry_start+retry_rows}")
            package_list = get_catalog_records(retry_start, retry_rows)
            
            if package_list is None:
                logger.error(f"❌ Final retry failed for range {retry_start}-{retry_start+retry_rows}")
                unresolved_failures.append((retry_start, retry_rows))
                continue
            elif not package_list:
                logger.info(f"ℹ️ No results for range {retry_start}-{retry_start+retry_rows} on retry")
                continue

            if not process_and_save_data(package_list, retry_start, retry_rows):
                unresolved_failures.append((retry_start, retry_rows))
                logger.error(f"❌ Final retry failed for range {retry_start}-{retry_start+retry_rows}")
            else:
                logger.info(f"✅ Successfully retried range {retry_start}-{retry_start+retry_rows}")
    else:
        logger.warning(f"⚠️ Record count changed from {initial_count} to {current_count}, skipping retries")
        unresolved_failures = failed_ranges  # Consider all failures as unresolved if count changed

# %%
# done
logger.info(f"✅ Completed: {time.time() - all_start:.2f} seconds")

# Raise exception if there were any unresolved failures
if unresolved_failures:
    failure_ranges_str = ", ".join([f"{start}-{start+rows}" for start, rows in failed_ranges])
    failure_ranges_unresolved_str = ", ".join([f"{start}-{start+rows}" for start, rows in unresolved_failures])
    raise RuntimeError(f"Failed to retrieve {len(unresolved_failures)} ranges after all retries:\n{failure_ranges_unresolved_str} \nOriginal ranges:\n{failure_ranges_str}")

