# %%
# imports and initialization
import copy
# import csv
import datetime
import json
import logging
import os

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s| %(message)s",
    datefmt="%H:%M:%S"
)


local_config = {
    "input": {
        "data_folder": "../../data/daily_statistics"
    },
    "output": {
        "web_data_folder": "../../web/static/data"
    }
}

sample_output = {
    "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "organization_count": 0,
    "catalog_daily_statistics": {
        "2020-01-01T00:00:00Z": {
            "added": 0,
            "removed": 0,
            "count": 0,
            "net_change": 0
        }
    },
    "resource_daily_statistics": {
        "2020-01-01T00:00:00Z": {
            "added": 0,
            "removed": 0,
            "total_count": 0,
            "total_delta": 0
        }
    },
    "organizations": []
}

output = {
    "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "organizations": {}
}

excluded_organization_keys = ['timeline', 'city']


logging.debug("Initialized")

# %%
# get the list of json files to iterate through, ordered by date (filename)
daily_json_files = [f for f in os.listdir(local_config["input"]["data_folder"]) if f.endswith(".json")]
daily_json_files.sort()
logging.info(f"Found {len(daily_json_files)} daily json files")


# %%
# iterate through the daily json files and generate the web data
for json_file in daily_json_files:
    logging.info(f"Processing {json_file}")
    with open(os.path.join(local_config["input"]["data_folder"], json_file), "r", encoding="utf-8") as f:
        daily_data = json.load(f)

    current_date = date_obj = datetime.datetime \
        .strptime(daily_data["date"], "%Y%m%dT%H%M%S") \
        .replace(tzinfo=datetime.timezone.utc)
    
    current_date_iso_string = current_date.isoformat()

    for org in daily_data["counts"]["organizations"]:
        if org["id"] not in output["organizations"]:
            output_org = { k: v for k, v in org.items() if k not in excluded_organization_keys }
            output_org["catalog_entry_counts"] = {}
            output_org["resource_entry_counts"] = {}
            del output_org["catalog_count"]
            del output_org["resource_count"]
            output["organizations"][org["id"]] = output_org
        else:
            output_org = output["organizations"][org["id"]]

        output_org["catalog_entry_counts"][current_date_iso_string] = org["catalog_count"]
        output_org["resource_entry_counts"][current_date_iso_string] = org["resource_count"]

    total_count = daily_data["counts"]["total_records"]
    additions = len(daily_data.get("deltas", {}).get("added", []))
    deletions = len(daily_data.get("deltas", {}).get("removed", []))


    logging.info(f"Total Datasets: { total_count }; Deletions: { deletions }; Additions: { additions}")
    

# sort organizations by organization.title property
output["organizations"] = dict(sorted(output["organizations"].items(), key=lambda x: x[1]["title"]))

# %%
# write the web data to the output folder


# write the organizations file
with open(os.path.join(local_config["output"]["web_data_folder"], "organizations.json"), "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False)

for id, org in output["organizations"].items():
    with open(os.path.join(local_config["output"]["web_data_folder"], "organizations", f"{id}.json"), "w", encoding="utf-8") as f:
        json.dump(org, f, ensure_ascii=False)

# %%
