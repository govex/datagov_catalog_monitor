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

output = {
    "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "organizations": {},
    "catalog_daily_statistics": [],
    "resource_daily_statistics": []
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
previous_catalog_count_by_org = {}
previous_resource_count_by_org = {}
previous_catalog_count = None
previous_resource_count = None

for json_file in daily_json_files:
    logging.info(f"Processing {json_file}")
    with open(os.path.join(local_config["input"]["data_folder"], json_file), "r", encoding="utf-8") as f:
        daily_data = json.load(f)

    current_date = date_obj = datetime.datetime \
        .strptime(daily_data["date"], "%Y%m%dT%H%M%S") \
        .replace(tzinfo=datetime.timezone.utc)
    
    current_date_iso_string = current_date.isoformat()

    # create a data object that counts daily_data["deltas"]["added"] and daily_data["deltas"]["removed  "] by organization
    added_by_org = {}
    for delta_added in daily_data.get("deltas", {}).get("added", []):
        added_by_org[delta_added["organization"]["id"]] = added_by_org.get(delta_added["organization"]["id"], 0) + 1

    removed_by_org = {}
    for delta_removed in daily_data.get("deltas", {}).get("removed", []):
        removed_by_org[delta_removed["organization"]["id"]] = removed_by_org.get(delta_removed["organization"]["id"], 0) + 1

    # TODO: the deltas don't align with the added and removed counts; need to find out why

    # get the total count of datasets and resources
    output["catalog_daily_statistics"].append({
        "t": current_date_iso_string,
        "n": daily_data["counts"]["total_records"],
        "Δ": daily_data["counts"]["total_records"] - previous_catalog_count if previous_catalog_count is not None else 0,
        "↑": sum(added_by_org.values()),
        "↓": sum(removed_by_org.values())
    })

    output["resource_daily_statistics"].append({
        "t": current_date_iso_string,
        "n": daily_data["counts"]["total_resources"],
        "Δ": daily_data["counts"]["total_resources"] - previous_resource_count  if previous_resource_count is not None else 0,
        "↑": sum(added_by_org.values()),
        "↓": sum(removed_by_org.values())
    })

    # save the previous values for the next iteration
    previous_catalog_count = daily_data["counts"]["total_records"]
    previous_resource_count = daily_data["counts"]["total_resources"]

    for org in daily_data["counts"]["organizations"]:
        output_org = output["organizations"].get(org["id"])
        if output_org is None:
            output_org = {k: v for k, v in org.items() if k not in excluded_organization_keys}
            output_org["catalog_entry_counts"] = []
            output_org["resource_entry_counts"] = []
            del output_org["catalog_count"]
            del output_org["resource_count"]
            output["organizations"][org["id"]] = output_org

        output_org["catalog_entry_counts"].append({
            "t": current_date_iso_string,
            "n": org["catalog_count"],
            "Δ": org["catalog_count"] - previous_catalog_count_by_org[org["id"]] if org["id"] in previous_catalog_count_by_org else 0,
            "↑": added_by_org[org["id"]] if org["id"] in added_by_org else 0,
            "↓": removed_by_org[org["id"]] if org["id"] in removed_by_org else 0
        })

        output_org["resource_entry_counts"].append({
            "t": current_date_iso_string,
            "n": org["resource_count"],
            "Δ": org["resource_count"] - previous_resource_count_by_org[org["id"]] if org["id"] in previous_resource_count_by_org else 0,
            "↑": added_by_org[org["id"]] if org["id"] in added_by_org else 0,
            "↓": removed_by_org[org["id"]] if org["id"] in removed_by_org else 0
        })

        previous_catalog_count_by_org[org["id"]] = org["catalog_count"]
        previous_resource_count_by_org[org["id"]] = org["resource_count"]

    total_count = daily_data["counts"]["total_records"]
    additions = sum(added_by_org.values())
    deletions = sum(removed_by_org.values())


    logging.info(f"Total Datasets: { total_count }; Deletions: { deletions }; Additions: { additions}")
    

# sort organizations by organization.title property
output["organizations"] = dict(sorted(output["organizations"].items(), key=lambda x: x[1]["title"]))

# %%
# write the web data to the output folder


# write the organizations file
with open(os.path.join(local_config["output"]["web_data_folder"], "organizations.json"), "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2, sort_keys=True)

for id, org in output["organizations"].items():
    with open(os.path.join(local_config["output"]["web_data_folder"], "organizations", f"{id}.json"), "w", encoding="utf-8") as f:
        json.dump(org, f, ensure_ascii=False, indent=2, sort_keys=True)

# %%
