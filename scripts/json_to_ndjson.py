# given an input and output folder, convert all json files
#  in the input folder to ldjson files in the output folder
# NOTE: LDJSON cannot have line breaks in the JSON strings!

# %%
# imports and initialization
import os
import json

source_dir = '../data/data_gov_catalog'
target_dir = '../data/data_gov_catalog_ndjson'

# %%
# converts a tree of json files to a tree of ldjson files
def convert_json_to_ldjson(source_dir, target_dir):
    for root, dirs, files in os.walk(source_dir):
        for filename in files:
            if filename.endswith('.json'):
                source_path = os.path.join(root, filename)
                with open(source_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                rel_path = os.path.relpath(root, source_dir)
                out_dir = os.path.join(target_dir, rel_path)
                os.makedirs(out_dir, exist_ok=True)

                out_name = os.path.splitext(filename)[0] + '.ndjson'
                out_path = os.path.join(out_dir, out_name)
                with open(out_path, 'w', encoding='utf-8') as out_f:
                    for item in data:
                        out_f.write(json.dumps(item) + '\n')

# scan a folder for ldjson files and check for improper line breaks
def check_ldjson_for_line_breaks(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.ldjson'):
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f, start=1):
                        try:
                            json.loads(line.strip())
                        except json.JSONDecodeError:
                            print(f"Invalid line break in {file_path}, line {i}")

# %%
# run the conversion
if __name__ == '__main__':
    convert_json_to_ldjson(source_dir, target_dir)


