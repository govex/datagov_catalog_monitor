# %%
# imports and initialization
import glob
import json
import logging
import pydash as py_
import os

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(name)s: %(message)s")

local_config = {
    "input": {
        "data_folder": "../../data/data_gov_catalog"
    },
    "output": {
        "statistics_folder": "../data/daily_statistics"
    }
}

# %%
# function definitions

# get the most recent catalog folder
def get_most_recent_catalog_folder(root_catalog_folder: str = None) -> str:
    if root_catalog_folder:
        folders = [os.path.join(root_catalog_folder, f) for f in os.listdir(root_catalog_folder) if os.path.isdir(os.path.join(root_catalog_folder, f))]
        folders.sort(key=lambda x: os.path.basename(x), reverse=True)
        if folders:
            return folders[0]
    return None

# get the list of json files in the folder
def get_json_file_list(path: str = None) -> list:
    if path:
        return glob.glob(f"{path}/*.json")
    return []

# get the list of error files in the folder
def get_error_file_list(path: str = None) -> list:
    if path:
        return glob.glob(f"{path}/errors/*.json")
    return []

# retrieve the file and convert to json
def get_json(file_path: str = None) -> list | dict:
    if file_path:
        with open(file_path, "r") as file:
            return json.load(file)
    return {}

# %%
# load the data to a local object
json_data_folder = get_most_recent_catalog_folder(local_config["input"]["data_folder"])
json_data_files = get_json_file_list(json_data_folder)
json_data = []

for file in json_data_files:
    file_data = get_json(file)
    if type(file_data) == list:
        json_data.extend(file_data)

# %%
# handle quality issues

# convert to a dict and de-dupe any records (can happen if the catalog updates during the extraction process)
json_data_dict = py_.key_by(json_data, "id")
if len(json_data_dict) != len(json_data):
    logging.warning(f"de-duped {len(json_data) - len(json_data_dict)} records")
json_data = py_.uniq_by(json_data, "id")

# # filter out records with the specific group id
# group_id_to_remove = "7d625e66-9e91-4b47-badd-44ec6f16b62b"
# json_data = [record for record in json_data if not any(group["id"] == group_id_to_remove for group in record.get("groups", []))]


# %%
# structural changes
# extract "Org Hierarchy" from "extras" and add it to the parent dict
for entry in json_data:

    # Extract the "publisher_hierarchy" value
    publisher_hierarchy = py_.find(entry["extras"], lambda x: x["key"] == "publisher_hierarchy")

    # Assign to main dictionary if found
    if publisher_hierarchy:
        entry["publisher_hierarchy"] = publisher_hierarchy["value"]

# %%
# calculate summary statistics

# count the number of records
record_count = len(json_data_dict)
logging.info(f"record count: {record_count}")

# count the number of unique organizations
organizations_dict = py_.key_by([d["organization"] for d in json_data if "organization" in d], "id")
organization_count = len(organizations_dict)
logging.info(f"organization count: {organization_count}")

# count the number of datasets per organization
organization_dataset_count = py_.count_by([d["organization"]["id"] for d in json_data if "organization" in d])

# count the number of unique publishers
publishers_dict = py_.key_by([d for d in json_data if "publisher_hierarchy" in d], "publisher_hierarchy")
publisher_count = len(publishers_dict)
logging.info(f"publisher count: {publisher_count}")

# count the number of datasets per publisher
publisher_dataset_count = py_.count_by([d["publisher_hierarchy"] for d in json_data if "publisher_hierarchy" in d])

# count the number of unique groups
groups_list = py_.flatten([d["groups"] for d in json_data if "groups" in d])
groups_dict = py_.key_by(groups_list, "id")
group_count = len(groups_dict)
logging.info(f"group count: {group_count}")

# count the number of datasets per group
group_dataset_count = py_.count_by([d["groups"][0]["id"] for d in json_data if "groups" in d and len(d["groups"]) > 0])

# count the number of unique tags
tags_list = py_.flatten([d["tags"] for d in json_data if "tags" in d])
tags_dict = py_.key_by(tags_list, "id")
tag_count = len(tags_dict)
logging.info(f"tag count: {tag_count}")


# %%
