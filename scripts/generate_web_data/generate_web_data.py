# %%
# imports and initialization
import copy
# import csv
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

organizations = {}

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

    current_date = daily_data["date"]

    for org in daily_data["counts"]["organizations"]:
        if org["id"] not in organizations:
            output_org = copy.deepcopy(org)
            output_org["catalog_entry_counts"] = []
            output_org["resource_entry_counts"] = []
            del output_org["catalog_count"]
            del output_org["resource_count"]
            organizations[org["id"]] = output_org
        else:
            output_org = organizations[org["id"]]

        output_org["catalog_entry_counts"].append(
            {
                "date": current_date,
                "count": org["catalog_count"]
            }
        )

        output_org["resource_entry_counts"].append(
            {
                "date": current_date,
                "count": org["resource_count"]
            }
        )

# sort organizations by organization.title property
organizations = dict(sorted(organizations.items(), key=lambda x: x[1]["title"]))

# %%
# write the web data to the output folder

# write the organizations file
with open(os.path.join(local_config["output"]["web_data_folder"], "organizations.json"), "w", encoding="utf-8") as f:
    json.dump(list(organizations.values()), f, ensure_ascii=False)
# %%
