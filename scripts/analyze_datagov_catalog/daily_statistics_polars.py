
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
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
_script_start = time.time()

# needed to support categoricals across multiple files
# pl.enable_string_cache()

local_config = {
    "input": {
        "data_folder": "../../data/data_gov_catalog_ndjson"
    },
    "output": {
        "statistics_folder": "../data/daily_statistics"
    }
}

# load the list of excluded organizations
with open("excluded_organizations.csv", "r", encoding="utf-8") as f:
    excluded_organizations = f.read().splitlines()

logging.debug(f"excluded organizations: {excluded_organizations}")

# %%
# functions

# get the most recent catalog folder
def get_recent_catalog_folder(root_catalog_folder: str = None, age: int = 0) -> str:
    if root_catalog_folder:
        folders = [os.path.join(root_catalog_folder, f) for f in os.listdir(root_catalog_folder) if os.path.isdir(os.path.join(root_catalog_folder, f))]
        folders.sort(key=lambda x: os.path.basename(x), reverse=True)
        if folders:
            return folders[age]
    return None

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

# scan a folder for ldjson files and check for improper line breaks
# the function returns True if any line breaks are found
def check_ldjson_for_line_breaks(folder_paths: list[str]) -> bool:
    return_value = False
    for folder_path in folder_paths:
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                if file_name.endswith(".ndjson"):
                    file_path = os.path.join(root, file_name)
                    with open(file_path, "r", encoding="utf-8") as f:
                        for i, line in enumerate(f, start=1):
                            try:
                                json.loads(line.strip())
                            except json.JSONDecodeError:
                                logging.error(f"Invalid line break in {file_path}, line {i}")
                                return_value = True
    return return_value

def filter_catalog(catalog: pl.LazyFrame, excluded_organizations: list[str]) -> pl.LazyFrame:
    return catalog \
        .unique(subset=["id"]) \
        .filter(
            ~pl.col("organization").struct.field("id").is_in(excluded_organizations)
        )

# TODO: implement
def collect_catalog_statistics(catalog: pl.LazyFrame) -> dict:
    statistics = {}


logging.debug("functions loaded")

# %%
# load the data to a local object
ldjson_folder = get_recent_catalog_folder(local_config["input"]["data_folder"])
ldjson_files = get_json_file_list(ldjson_folder)

ldjson_older_folder = get_recent_catalog_folder(local_config["input"]["data_folder"], age=1)
ldjson_older_files = get_json_file_list(ldjson_older_folder)

logging.debug("file lists loaded")

# %%
# scan the relevant files for invalid line breaks
if check_ldjson_for_line_breaks([ldjson_folder, ldjson_older_folder]):
    # end the script if any invalid line breaks are found
    logging.error("Invalid line breaks found in the ldjson files.")
    raise SystemExit

logging.debug("line breaks checked")

# %%
# load the data into a polars lazyframes
# this is done to avoid loading the entire dataset into memory
catalog = pl.scan_ndjson(ldjson_files, ignore_errors=True)
catalog_older = pl.scan_ndjson(ldjson_older_files, ignore_errors=True)

logging.debug("data lazy loaded")

# %%
# set up filters and queries
catalog_unique = catalog.unique(subset=["id"])
catalog_unique_filtered = catalog_unique.filter(
    ~pl.col("organization").struct.field("id").is_in(excluded_organizations)
)

catalog_older_unique = catalog_older.unique(subset=["id"])
catalog_older_unique_filtered = catalog_older_unique.filter(
    ~pl.col("organization").struct.field("id").is_in(excluded_organizations)
)

logging.debug("filters set up")

# %%
# get the number of catalog entries by organization
catalog_counts_by_organization = (
    catalog_unique_filtered \
        .group_by(pl.col("organization").struct.field("id").alias("organization_id")) \
        .agg(pl.len().alias("catalog_entry_count"))
).collect()

logging.debug(f"catalog counts by organization: {catalog_counts_by_organization.height}")

# %%
# collect unique organizations from both datasets
organizations = catalog_unique_filtered.select("organization").unnest("organization").unique("id").collect()
logging.debug(f"organizations: {organizations.height}")
organizations_older = catalog_older_unique_filtered.select("organization").unnest("organization").unique("id").collect()
logging.debug(f"organizations_older: {organizations_older.height}")

# merge the two lists and de-duplicate
organizations_merged = pl.concat([organizations, organizations_older]).unique("id")

logging.debug(f"organizations_merged: {organizations_merged.height}")

# %%
# get the number of records in each catalog
catalog_unique_filtered_count = catalog_unique_filtered.select(pl.len()).collect().item()
logging.info(f"latest catalog federal record count: {catalog_unique_filtered_count}")

catalog_older_unique_filtered_count = catalog_older_unique_filtered.select(pl.len()).collect().item()
logging.info(f"older catalog federal record count: {catalog_older_unique_filtered_count}")

# %% 
# get the difference between the two catalogs

# Rows in the new snapshot that were not in the old (added rows)
added = catalog_unique_filtered.join(catalog_older_unique, on="id", how="anti")
added_df = added.collect()
logging.info(f"rows added: {added_df.height}")

# Rows in the old snapshot that are missing in the new (removed rows)
removed = catalog_older_unique_filtered.join(catalog_unique, on="id", how="anti")
removed_df = removed.collect()
logging.info(f"rows removed: {removed_df.height}")

# %%
# save the statistics
logging.info("statistics saved")

# %%
# wrap up
elapsed = time.time() - _script_start
formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed))
logging.info(f"Elapsed time " + formatted)
# %%
