import os
import sys
import json

gt_data = sys.argv[1]
result_data = sys.argv[2]
final_result_data = sys.argv[3]
f_gt = open(gt_data, 'r')

data = json.load(f_gt)
f_result = open(final_result_data, 'w')
results = []
with open(result_data, 'r') as f:
    data_t = json.load(f)
    for item in data_t:
        #item = json.loads(line)
        idx = item['id'] - 1
        item['level'] = data[idx]['level']
        #item['id'] = item['id']
        results+=[item]
#f_result.write('{}\n'.format(json.dumps(item)))
json.dump(results,f_result)
