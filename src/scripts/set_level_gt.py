import os
import sys
import json


gt_data = sys.argv[1]
level_data = sys.argv[2]
output_gt = sys.argv[3]

f_gt = open(gt_data, 'r')
f_level = open(level_data, 'r')

data  = json.load(f_gt)
for i, line in enumerate(f_level):
    if i == 0:
        continue
    level = line.split('|')[1]
    data[i-1]['level'] = level


with open(output_gt, 'w') as f:
    json.dump(data, f, indent=4)