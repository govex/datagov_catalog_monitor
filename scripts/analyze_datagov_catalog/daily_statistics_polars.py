
# %%
# imports and initialization
import glob
import json
import logging
import polars as pl
import os
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s| %(message)s",
    datefmt="%H:%M:%S"
)
_script_start = time.time()

logging.debug("startng up...")

# needed to support categoricals across multiple files
# pl.enable_string_cache()

local_config = {
    "input": {
        "data_folder": "../../data/data_gov_catalog_ndjson"
    },
    "output": {
        "statistics_folder": "../../data/daily_statistics"
    }
}

# load the list of excluded organizations
with open("excluded_organizations.csv", "r", encoding="utf-8") as f:
    excluded_organizations = f.read().splitlines()

logging.debug(f"excluded organizations: {len(excluded_organizations)}")

# %%
# functions

# get the most recent catalog folders going back the specified number of cycles
# cycles usually means days but that's not a strict rule;
def get_recent_catalog_folders(root_catalog_folder: str = None, cycles: int = 1) -> str:
    if root_catalog_folder:
        folders = [os.path.join(root_catalog_folder, f) for f in os.listdir(root_catalog_folder) if os.path.isdir(os.path.join(root_catalog_folder, f))]
        folders.sort(key=lambda x: os.path.basename(x), reverse=True)
        if folders:
            return folders[:cycles+1]
    return None

def get_date_from_folder_name(folder_name: str = None) -> str:
    if folder_name:
        return os.path.basename(folder_name)

# get the list of json files in the folder
def get_json_file_list(path: str = None) -> list:
    if path:
        return glob.glob(f"{path}/*.ndjson")
    return []

# get the list of error files in the folder
def get_error_file_list(path: str = None) -> list:
    if path:
        return glob.glob(f"{path}/errors/*.json")
    return []

# retrieve a json file and parse
def get_json(file_path: str = None) -> list | dict:
    if file_path:
        with open(file_path, "r") as file:
            return json.load(file)
    return {}

# filter the catalog to remove duplicates and excluded organizations
def filter_catalog(catalog: pl.LazyFrame, excluded_organizations: list[str] = []) -> pl.LazyFrame:
    return catalog \
        .unique(subset=["id"]) \
        .filter(
            ~pl.col("organization").struct.field("id").is_in(excluded_organizations)
        )

# collect statistics on the catalog; assumes any filtering has already been done
def collect_catalog_info(catalog: pl.LazyFrame) -> dict:
    catalog_info = {}

    catalog_counts_by_organization = catalog \
        .group_by(pl.col("organization").struct.field("id").alias("organization_id")) \
        .agg([
            pl.col("organization").first().alias("organization"),
            pl.len().alias("catalog_count"),
            pl.col("resources").list.len().sum().alias("resource_count")
        ]) \
        .collect()
    
    catalog_info["total_records"] = catalog_counts_by_organization.select("catalog_count").sum().item()
    catalog_info["total_resources"] = catalog_counts_by_organization.select("resource_count").sum().item()
    catalog_info["organizations"] = catalog_counts_by_organization.unnest("organization").to_dicts()

    return catalog_info

def get_catalog_differences(older: pl.LazyFrame, newer: pl.LazyFrame) -> dict:

    # there is an alternate approach: full outer join + filtering, which only
    # requires one pass through the data, but it may require more memory and time
    added = newer \
        .join(older, on="id", how="anti") \
        .collect()
    removed = older \
        .join(newer, on="id", how="anti") \
        .collect()

    return {
        "added": added.to_dicts(),
        "removed": removed.to_dicts()
    }

logging.debug("functions loaded")

# %%
# get with the work and output the results

# this function call supports a cycles parameter to go back further than the default 1
folders = get_recent_catalog_folders(local_config["input"]["data_folder"])

os.makedirs(local_config["output"]["statistics_folder"], exist_ok=True)

for i in range(len(folders) - 1):
    logging.debug(f"processing {folders[i]}...")
    ndjson_files = get_json_file_list(folders[i])
    ndjson_older_files = get_json_file_list(folders[i + 1])

    # initialize the lazyframes for the current data and prior cycle
    catalog = filter_catalog(pl.scan_ndjson(ndjson_files, ignore_errors=True), excluded_organizations=excluded_organizations)
    catalog_older = filter_catalog(pl.scan_ndjson(ndjson_older_files, ignore_errors=True), excluded_organizations=excluded_organizations)

    datetimestring = get_date_from_folder_name(folders[i])

    # generate the result object (this can take some time for each pass)
    result = {
        "date": datetimestring,
        "current_fileset": folders[i],
        "comparison_fileset": folders[i + 1],
        "counts": collect_catalog_info(catalog),
        "deltas": get_catalog_differences(older=catalog_older, newer=catalog)
    }

    # output the result
    filename = os.path.join(local_config["output"]["statistics_folder"], f"{datetimestring}.json")
    with open(filename, mode="w") as file:
        json.dump(result, file)
        logging.debug(f"saved statistics to {file.name}...")
        logging.debug(f"added: {len(result['deltas']['added'])}, removed: {len(result['deltas']['removed'])}")


# %%
# wrap up
elapsed = time.time() - _script_start
formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed))
logging.info(f"Elapsed time " + formatted)
# %%
