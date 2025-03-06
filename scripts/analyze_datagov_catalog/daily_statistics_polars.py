# %%
# imports and initialization
import glob
import json
import logging
import polars as pl
import os
from pathlib import Path
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
        "data_folder": Path(os.environ["DATA_CACHE_DIR"]) / "data_gov_catalog_ndjson"
    },
    "output": {
        "statistics_folder": Path(os.environ["DATA_CACHE_DIR"]) / "daily_statistics"
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
def get_recent_catalog_folders(root_catalog_folder: Path = None, cycles: int = 1) -> list[Path]:
    if root_catalog_folder:
        folders = [p for p in root_catalog_folder.iterdir() if p.is_dir()]
        folders.sort(key=lambda x: x.name, reverse=True)
        if folders:
            return folders[:cycles+1]
    return None

def get_date_from_folder_name(folder_path: Path = None) -> str:
    if folder_path:
        return folder_path.name

# get the list of json files in the folder
def get_json_file_list(path: Path = None) -> list[Path]:
    if path:
        return list(path.glob("*.ndjson"))
    return []

# get the list of error files in the folder
def get_error_file_list(path: Path = None) -> list[Path]:
    if path:
        return list(path.glob("errors/*.json"))
    return []

# retrieve a json file and parse
def get_json(file_path: Path = None) -> list | dict:
    if file_path:
        return json.loads(file_path.read_text())
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

local_config["output"]["statistics_folder"].mkdir(parents=True, exist_ok=True)

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
        "current_fileset": str(folders[i]),
        "comparison_fileset": str(folders[i + 1]),
        "counts": collect_catalog_info(catalog),
        "deltas": get_catalog_differences(older=catalog_older, newer=catalog)
    }

    # output the result
    filename = local_config["output"]["statistics_folder"] / f"{datetimestring}.json"
    filename.write_text(json.dumps(result))
    logging.debug(f"saved statistics to {filename}...")
    logging.debug(f"added: {len(result['deltas']['added'])}, removed: {len(result['deltas']['removed'])}")


# %%
# wrap up
elapsed = time.time() - _script_start
formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed))
logging.info(f"Elapsed time " + formatted)
# %%
