import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler
from utils import ConfigHandler
import json
import string

# 0.005,0.007,0.008,0.002,
b_values = [0.08,0.1,0.12,0.2,0.3]
for value in b_values:
    config_obj = {}
    with open("../../config/yelp_config.json", ) as f:
        config_obj = json.load(f)

    config_obj['normalization_value'] = value
    config_obj['evaluation'][0]['result_file'] = "../../data/results/new/yelp_result_b_{}.json".format(value)
    print(value == 0.005)
    #if value == 0.005:
    #
    # +    config_obj['evaluation'][0]['start_from'] = 37

    with open('../../config/yelp_config.json', 'w') as f:
        json.dump(config_obj, f,indent=4)

    print("Running for b = {}".format(value))
    config = ConfigHandler(reset=True)
    evaluation = EvaluationHandler()
    #evaluation.re_run_evaluation()
    evaluation.load_query_file()
    evaluation.run_evaluation()
    evaluation.calculate_metrics()
    evaluation.save_gt()