import sys
import os
import json

result_file = sys.argv[1]
gt_file = sys.argv[2]
result_gt_file = sys.argv[3]


ids = []
result_gt = []
with open(result_file) as f:
    ids = [json.loads(line)['query_id'] for line in f]

with open(gt_file) as f:
    complete_gt = json.load(f)
    result_gt = [gt_item for gt_item in complete_gt if gt_item['id']  in ids]

with open(result_gt_file, 'w') as f:
    json.dump(result_gt, f, indent=4)