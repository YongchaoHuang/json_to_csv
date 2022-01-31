import json
import pandas as pd

wd = '/your_filepath_/'

df = pd.read_csv(wd+"input.csv")
# df = df.groupby(["cycle", "site", "organisation", "impactAssessment", "source", "actor", "term"], as_index=False).agg(lambda x: list(x))

json_data = df.to_dict(orient="records")

def unflatten_dic(dic):
    for k, v in list(dic.items()):
        subkeys = k.split('.')
        if len(subkeys) > 1:
            dic.setdefault(subkeys[0], dict())
            dic[subkeys[0]].update({"".join(subkeys[1:]): v})
            unflatten_dic(dic[subkeys[0]])
            del (dic[k])


def merge_lists(dic):
    for k, v in list(dic.items()):
        if isinstance(v, dict):
            keys = list(v.keys())
            vals = list(v.values())
            if all(isinstance(l, list) and len(l) == len(vals[0]) for l in vals):
                dic[k] = []
                val_tuple = set(zip(*vals))  # removing duplicates with set()
                for t in val_tuple:
                    dic[k].append({subkey: t[i] for i, subkey in enumerate(keys)})
            else:
                merge_lists(v)
        elif isinstance(v, list):
            dic[k] = list(set(v))  # removing list duplicates


for user in json_data:
    unflatten_dic(user)
    merge_lists(user)

print(json.dumps(json_data, indent=4))

with open(wd+'json_data.json', 'w') as outfile:
    json.dump(json_data, outfile)

import pandas as pd
pdObj = pd.read_json(wd+'json_data.json', orient='records')
pdObj.to_csv(wd+'saved_json_data.csv')

# re-write pandas to csv
import csv
# now we will open a file for writing
data_file = open(wd+'data_file.csv', 'w')
# create the csv writer object
csv_writer = csv.writer(data_file)
count = 0
for emp in json_data:
    if count == 0:
        # Writing headers of CSV file
        header = emp.keys()
        csv_writer.writerow(header)
        count += 1

    # Writing data of CSV file
    csv_writer.writerow(emp.values())



