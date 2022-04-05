import json
import sys
sys.path.append('../')
from keyword_match import KeywordMatch
gt_file = sys.argv[1]
result_file = sys.argv[2]
csv_file = sys.argv[3]
gt_out_from_top_file = sys.argv[4]
result_file_gt = sys.argv[5]


f_result = open(csv_file, 'w')

gt_data = []
gt_out_from_top = []
with open(gt_file) as f:
    gt_data = json.load(f)

result_data = []
with open(result_file) as f:
    for line in f:
        item = json.loads(line)
        if item['query_candidate_item'] > 4:
            print(item['query_id'])
            idx = item['query_id'] - 1
            mappings = []
            gt_out_from_top += [gt_data[idx]]
            result_data += [item]

            for qm in gt_data[idx]['query_matches']:
                km = KeywordMatch.from_json_serializable(qm)
                mappings += ['{}:{}.{}:{}'.format(i[3], i[1], i[2], 'VT' if i[0] == 'v' else 'NT') for i in km.kw_mappings()]
            
            line = '=substitute("{}";"|"; char(10))'.format('|'.join(mappings))
            f_result.write("{}\t{}\t{}\t{}\n".format(gt_data[idx]['id'], gt_data[idx]['natural_language_query'],  line, item['query_candidate_item']))

with open(gt_out_from_top_file, 'w') as f:
    json.dump(gt_out_from_top, f)

with open(result_file_gt, 'w') as f:
    for item in result_data:
        f.write('{}\n'.format(json.dumps(item)))
    #json.dump(result_data, f)


