# this script scans the daily statistics files and finds datasets which have been removed and then returned
# it then outputs a list of datasets which have been removed and returned
# %%
# imports and initialization
import glob
import json
import logging
import os
from pathlib import Path
import time
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s| %(message)s",
    datefmt="%H:%M:%S"
)

match_field = "title"

_script_start = time.time()

# Get cache directory from environment variable
CACHE_DIR = Path(os.getenv('DATA_CACHE_DIR')) / 'daily_statistics'
if not CACHE_DIR:
    raise ValueError("DATA_CACHE_DIR environment variable not set")

def get_date_from_filename(filename: Path):
    """Extract date from filename like '20250310T070248.json'"""
    base = os.path.basename(filename)
    date_str = base.replace('stats_', '').replace('.json', '')
    return datetime.strptime(date_str, '%Y%m%dT%H%M%S')

# Get all JSON files and sort them by date
json_files = glob.glob(os.path.join(CACHE_DIR, '*.json'))
json_files.sort(key=get_date_from_filename)

if not json_files:
    logging.error(f"No JSON files found in {CACHE_DIR}")
    exit(1)

# Track removed datasets and when they were removed
removed_catalog_ids = set()  # {dataset_id: removal_date}
returned_catalog_ids = []  # datasets that were removed and later returned
removed_total_count = 0
added_total_count = 0


# Process each file chronologically
for json_file in json_files:
    date = get_date_from_filename(json_file)
    logging.info(f"Processing {json_file}")
    
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    # Get the list of removed datasets for this day
    removals = set(entry[match_field] for entry in data.get('deltas', {}).get('removed', []))
    removed_catalog_ids.update(removals)
    removed_total_count += len(removals)
    
    additions = set(entry[match_field] for entry in data.get('deltas', {}).get('added', []))
    added_total_count += len(additions)
    
    for dataset_id in additions:
        if dataset_id in removed_catalog_ids:
            returned_catalog_ids.append({dataset_id: date})
            removed_catalog_ids.remove(dataset_id)
    
# Report results
logging.info(f"Found {len(returned_catalog_ids)} datasets that were removed and later returned")
logging.info(f"Removed {removed_total_count} datasets")
logging.info(f"Added {added_total_count} datasets")

script_duration = time.time() - _script_start
logging.info(f"Script completed in {script_duration:.2f} seconds")
