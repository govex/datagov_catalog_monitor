# %%
# imports and initialization
import ckanapi
from datetime import datetime, UTC
import json
import logging
import os
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
import boto3

# configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)8s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('s3transfer.tasks').setLevel(logging.WARNING)
logging.getLogger('s3transfer.utils').setLevel(logging.WARNING)


# Get environment variables
aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]
cache_dir = Path(os.environ["DATA_CACHE_DIR"]) / 'data_gov_catalog_ndjson'
bucket_name = os.environ.get("S3_BUCKET_NAME", "govex-us-data-archive")
s3_base_prefix = "Catalog/data_gov_catalog_ndjson"

# initialize output directory
current_utc_time = datetime.now(UTC).strftime('%Y%m%dT%H%M%S')
local_output_dir = cache_dir / current_utc_time
os.makedirs(local_output_dir, exist_ok=True)

# initialize ckan api
ckan = ckanapi.RemoteCKAN('https://catalog.data.gov')

# Initialize Boto3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60),
       before_sleep=lambda retry_state: logging.debug(f"⭕️ S3 upload attempt {retry_state.attempt_number} failed, retrying in {retry_state.next_action.sleep} seconds"))
def upload_to_s3(file_path, s3_key):
    """Upload a file to S3 with retry logic."""
    try:
        s3.upload_file(str(file_path), bucket_name, s3_key)
        logging.info(f"✅ Uploaded {s3_key} to S3")
    except Exception as e:
        logging.debug(f"⭕️ S3 upload failed with error: {str(e)}")
        raise e

# %%
# load organizations from catalog
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
def get_organization_batch(limit=25, offset=0):
    try:
        logging.debug(f'Retrieving organizations from offset {offset}')
        return ckan.action.organization_list(
            include_dataset_count=True,
            all_fields=True,
            include_extras=True,
            include_groups=True,
            include_tags=True,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        logging.debug(f"⭕️ Attempt {e.retry_state.attempt_number} errored: {str(e)}")
        raise e

# iterate to retreive all organizations; we know we are done when the number of organizations returned is less than the limit
limit = 25 # default limit for ckan is 25
offset = 0
organizations = []
while True:
    org_batch = get_organization_batch(limit, offset)
    organizations.extend(org_batch)
    if len(org_batch) < limit:
        break
    offset += limit

# sort organizations by package count
organizations.sort(key=lambda x: x['package_count'])

# write organizations to json file
orgs_file = local_output_dir / '_organizations.json'
with open(orgs_file, 'w') as f:
    json.dump(organizations, f)

# Upload organizations file to S3
s3_key = f"{s3_base_prefix}/{current_utc_time}/_organizations.json"
try:
    upload_to_s3(orgs_file, s3_key)
except Exception as e:
    logging.warning(f"❌ All retry attempts failed for uploading organizations.json to S3: {str(e)}")

# %%
# filter results to only include federal organizations
def organization_is_federal(organization):
    for extra in organization.get('extras', []):
        if extra.get('key') == 'organization_type' and extra.get('value') == 'Federal Government':
            return True
    return False

federal_organizations = [org for org in organizations if organization_is_federal(org)]

# write federal organizations to json file
fed_orgs_file = local_output_dir / '_federal_organizations.json'
with open(fed_orgs_file, 'w') as f:
    json.dump(federal_organizations, f)

# Upload federal organizations file to S3
s3_key = f"{s3_base_prefix}/{current_utc_time}/_federal_organizations.json"
try:
    upload_to_s3(fed_orgs_file, s3_key)
except Exception as e:
    logging.warning(f"❌ All retry attempts failed for uploading federal_organizations.json to S3: {str(e)}")

# %%
# get datasets for each federal organization
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60),
       before_sleep=lambda retry_state: logging.debug(f"⭕️ Attempt {retry_state.attempt_number} failed, retrying in {retry_state.next_action.sleep} seconds"))
def get_organization_packages(org_name, start=0, rows=1000):
    try:
        logging.debug(f"Retrieving packages for {org_name} (start={start}, rows={rows})")
        return ckan.action.package_search(
            q=f"organization:{org_name}",
            rows=rows,
            start=start
        )
    except Exception as e:
        logging.warning(f"⭕️ Attempt failed with error: {str(e)}")
        raise e
    

# %%
# get datasets for each federal organization

for org in organizations:
    all_packages = []
    start = 0
    rows = 100
    expected_count = org['package_count']
    logging.info(f"Retrieving {org['name']}: {org['package_count']} packages expected")
    
    while True:
        try:
            response = get_organization_packages(org['name'], start=start, rows=rows)
            packages = response['results']
            all_packages.extend(packages)
            
            # If we've retrieved all datasets, break the loop
            if start + len(packages) >= response['count']:
                break
                
            start += len(packages)
        except Exception as e:
            logging.warning(f"❌ Error retrieving datasets for organization {org['name']}: {str(e)}")
            break
    
    # write datasets to ndjson file 
    output_file = local_output_dir / f"{org['name']}.ndjson"
    with open(output_file, 'w') as f:
        for package in all_packages:
            f.write(json.dumps(package) + '\n')

    # Upload to S3
    s3_key = f"{s3_base_prefix}/{current_utc_time}/{org['name']}.ndjson"
    try:
        upload_to_s3(output_file, s3_key)
    except Exception as e:
        logging.error(f"❌ All retry attempts failed for uploading {org['name']}.ndjson to S3: {str(e)}")

    logging.info(f"✅ Retrieved {len(all_packages)} datasets for organization {org['name']} (expected {org['package_count']})")


# %%
