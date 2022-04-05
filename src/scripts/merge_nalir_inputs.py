import os
import sys
import json


nalir_inputs_file = sys.argv[1]
nalir_output_file = sys.argv[2]
#nalir_output_two_file = sys.argv[3]
result_k2d_file = sys.argv[3]
result_nalir_file = sys.argv[4]
gt_data = []

with open(nalir_inputs_file, 'r') as f:
    gt_data = json.load(f)
gt_data_dict = {r['id']: r for r in gt_data}

f_result = open(result_nalir_file, 'w')


with open(result_k2d_file, 'r') as f:
    for line in f:
        record = json.loads(line)
        if gt_data_dict[record['query_id']]['equal_gt']:
            f_result.write('{}\n'.format(json.dumps(record)))


with open(nalir_output_file, 'r') as f:
    for line in f:
        record = json.loads(line)
        # record['query_id'] = record['id']
        # del record['id']
        f_result.write('{}\n'.format(json.dumps(record)))

# with open(nalir_output_two_file, 'r') as f:
#     for line in f:
#         record = json.loads(line)
#         print(record)
#         f_result.write('{}\n'.format(json.dumps(record)))
