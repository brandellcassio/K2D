import sys
import os
sys.path.append('../')
from mapper import Mapper
from utils import ConfigHandler
import json
import string

def get_default_attributes():
    attribute_map={}
    with open(config.relations_file, "r") as f:
        attribute_map = {item['name']:attr['name']  for item in json.load(f) \
            for attr in item['attributes'] if attr.get('importance','') == 'primary' }
    
    return attribute_map

def fix_matches(ranked_matches):
    attribute_map = get_default_attributes()
    for i, ranked_match in enumerate(ranked_matches): 
        for kw_match in ranked_match.matches:
            if kw_match.has_default_mapping() and kw_match.table in attribute_map:
                kw_match.replace_default_mapping(attribute_map[kw_match.table])
    
    return ranked_matches

b_values = [0.5]
for value in b_values:
    config_obj = {}
    with open("../../config/yelp_config.json", ) as f:
        config_obj = json.load(f)

    config_obj['normalization_value'] = value

    with open('../../config/yelp_config.json', 'w') as f:
        json.dump(config_obj, f,indent=4)

    result_mapper = Mapper()
    file_data = "../../data/nalir_inputs/yelp_nalir_inputs.v01.json"
    file_output = "../../data/results/yelp_inputs_result_b_{}.json".format(value)
    #from_output = int(sys.argv[3]) if len(sys.argv) == 4 else 0
    output_fp = open(file_output, 'a+')
    results = {}
    print("Running for b = {}".format(value))
    config = ConfigHandler()
    with open(file_data) as f:
        json_data = json.load(f)
        for i, item in enumerate(json_data):
            print(i)
            if  not item['equal_gt']:
                print("running for query {}".format(i))
                print(file_output)
                
                segments = [x.replace('.', '') for x in item['segments'] ]
                keys = segments[:]
                keys.sort(key=lambda x: x)
                key = ':'.join(keys)
                
                match = None
                if key in results:
                    match = results[key]                    
                else:
                    qm_results = result_mapper.get_matches(segments)
                    matches = fix_matches(qm_results)
                    results[key] = matches
                    
                json_data[i]['query_matches'] = [json.loads(x.to_json()) for x in matches]
                print('{}\n'.format(json.dumps(json_data[i])))
                output_fp.write('{}\n'.format(json.dumps(json_data[i])))
                output_fp.flush()