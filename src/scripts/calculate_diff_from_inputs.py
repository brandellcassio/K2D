import sys
import os
import json
import sys

gt_file = sys.argv[1]
nalir_input_file = sys.argv[2]
output_file = sys.argv[3]

json_gt_file = []
with open(gt_file, 'r') as f:
    json_gt_file = json.load(f)

json_input_file = []
with open(nalir_input_file, 'r') as f:
    json_input_file = json.load(f) 
count = 0
print(len(json_input_file), len(json_gt_file))
for i, item_gt in enumerate(json_gt_file):
    nalir_input_segments = set([x.lower() for x in json_input_file[i]['segments']])
    gt_segments = set([x.lower() for x in item_gt['segments']])
    json_input_file[i]["id"] = i + 1
    print(gt_segments,nalir_input_segments, gt_segments == nalir_input_segments)
    if gt_segments == nalir_input_segments:
        json_input_file[i]['equal_gt'] = True
        count+=1
    else:
        json_input_file[i]['equal_gt'] = False
print(count)
with open(output_file, 'w') as f:
    json.dump(json_input_file, f)
