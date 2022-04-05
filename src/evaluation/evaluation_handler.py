from mapper import Mapper
from copy import deepcopy
import json
import sys
from copy import deepcopy

from utils import ConfigHandler
from evaluation.evaluation_item import EvaluationItem


class EvaluationHandler:
    
    def __init__(self):
        self.mapper = Mapper()
        self.config = ConfigHandler()
        self.evaluation_items = []
        self.global_precision = []
        self.global_mrr = 0.0
        self.start_from = 0
        self.evaluation_item_dict = {}
    
    
    def load_query_file(self):
        json_file = self.config.evaluation[self.config.default_evaluation_index]['query_file']
        print("loading query file...")
        with open(json_file) as f:
            data = json.load(f)    
            for i, item in enumerate(data):
                evaluation_item = EvaluationItem()
                evaluation_item.build_from_json(item)
                self.evaluation_items += [evaluation_item]
    
        self.start_from = self.config.evaluation[self.config.default_evaluation_index]['start_from']
        self.result_file =  self.config.evaluation[self.config.default_evaluation_index]['result_file']
        self.min_segments = self.config.evaluation[self.config.default_evaluation_index]['min_segments']
        self.max_segments = self.config.evaluation[self.config.default_evaluation_index]['max_segments']
    

    def evaluate_nalir(self, with_levels=True):
        nalir_file = self.config.evaluation[self.config.default_evaluation_index]['nalir_file']
        nalir_evaluation_items = []
        metrics_by_level = {}
        precision = 0
        queries = 0    
        with open(nalir_file, 'r') as f:
            nalir_data = json.load(f)
            for i, nalir_item in enumerate(nalir_data):
                nalirEvalItem = EvaluationItem()
                nalirEvalItem.build_from_nalir_json(nalir_item)
                item = deepcopy(nalir_item)
                print(item)
                if with_levels:
                    level_size, level_precision = metrics_by_level.setdefault(item['level'], (0,0))
                
                
                if nalirEvalItem == self.evaluation_items[i]:
                    item['query_candidate_item'] = 0
                    precision += 1
                    if with_levels:
                        level_precision+=1
                else:
                    item['query_candidate_item'] = -1
                
                queries+=1
                if with_levels:
                    level_size +=1
                    metrics_by_level[item['level']] = (level_size, level_precision)
                nalir_evaluation_items += [item]
                
        with open(nalir_file, 'w') as f:
            json.dump(nalir_evaluation_items, f)
        
        print("Precision: {}, {}, {}".format(precision/queries, precision, queries))
        if with_levels:
            for level in metrics_by_level:
                size, metric = metrics_by_level[level]
                print('{} {:.5f}'.format(level, metric/size))

                                 
    def run_evaluation(self):
        print("running evaluation...")
        #indices = [75]
        for i, evaluation_item in enumerate(self.evaluation_items):
            if evaluation_item.id < self.start_from or (self.min_segments > len(evaluation_item.segments) or len(evaluation_item.segments) > self.max_segments):
                continue
            print(f"running for {evaluation_item.segments}, {len(evaluation_item.segments)} id: {evaluation_item.id} ")
            key = frozenset(evaluation_item.segments)
            if key in self.evaluation_item_dict:
                print("result cached")
                ranked_match, qm_position = self.evaluation_item_dict[key]
                self.evaluation_items[i].ranked_matches = ranked_match
                self.evaluation_items[i].qm_position = qm_position
            else:
                ranked_items = self.mapper.get_matches(evaluation_item.segments)
                #max_items = 30 if len(ranked_items) > 30 else len(ranked_items)
                self.evaluation_items[i].ranked_matches = self._fix_matches(ranked_items)
                self.evaluation_items[i].find_query_match_gt()
                self.evaluation_item_dict[key] = (evaluation_item.ranked_matches, evaluation_item.qm_position)
            self.store_partial_result(self.evaluation_items[i])

            
    def calculate_metrics(self):
        self._get_precision()
        self._get_mrr()        
        
    
    def _get_precision(self):
        positions = [0.0] * 6
        for eval_item in self.evaluation_items:
            pos = 5 if eval_item.qm_position == -1 or eval_item.qm_position >= 5 else eval_item.qm_position
            positions[pos] += 1.0
        
        precion_metrics = [pos/(len(self.evaluation_items)* 1.0) for pos in positions]
        
        self.global_precision = precion_metrics
        
    def _get_mrr(self):
        mrr = 0.0
        for eval_item in self.evaluation_items:
             mrr += 1/(1.0*eval_item.qm_position) if eval_item.qm_position > 0 else 0.0
        mrr = mrr / (len(self.evaluation_items) * 1.0)
        
        self.global_mrr = mrr

    def save_gt(self):
        print(self.global_mrr, self.global_precision)

    def store_partial_result(self, eval_item):
        with open(self.result_file, 'a+') as f:
            result = '[{}]'.format(','.join([x.to_json() for x in eval_item.ranked_matches]))
            match_item = {'query_candidate_matches': []}
            match_item['query_matches'] = json.loads(result)
            match_item['query_candidate_item'] = eval_item.qm_position
            match_item['query_id'] = eval_item.id
            match_item['query_scores'] = [x.total_score for x in eval_item.ranked_matches]
            print("QM POSITION: ", eval_item.qm_position)
            f.write('{}\n'.format(json.dumps(match_item)))

    def _get_default_attributes(self):
        attribute_map={}
        with open(self.config.relations_file, "r") as f:
            attribute_map = {item['name']:attr['name']  for item in json.load(f) \
                for attr in item['attributes'] if attr.get('importance','') == 'primary' }
        return attribute_map

    def _fix_matches(self, ranked_matches):
        attribute_map = self._get_default_attributes()
        for i, ranked_match in enumerate(ranked_matches): 
            for kw_match in ranked_match.matches:
                if kw_match.has_default_mapping() and kw_match.table in attribute_map:
                    kw_match.replace_default_mapping(attribute_map[kw_match.table])

        return ranked_matches

    def read_file(self, key_file,with_levels=True):
        self.result_file =  self.config.evaluation[self.config.default_evaluation_index][key_file]
        precision = [0.0]*6
        metrics_by_level = {}
        mrr = 0.0
        size = 0
        with open(self.result_file) as f:
            for line in f:
                item = json.loads(line)
                #print()
                if with_levels:
                    level_mrr,level_size, level_precision = metrics_by_level.setdefault(item['level'], (0.0,0,[0.0] * 6))
                
                pos = item['query_candidate_item'] if item['query_candidate_item'] < 5 and item['query_candidate_item'] >= 0 else -1
                
                precision[pos] +=1
                size+=1
                if with_levels:
                    level_precision[pos] += 1
                    level_size +=1
                
                
                

                if item['query_candidate_item'] != -1:
                    local_mrr =  (1/(item['query_candidate_item']+1))
                    mrr += local_mrr
                    if with_levels:
                        level_mrr +=local_mrr
                
                if with_levels:
                    metrics_by_level[item['level']] = (level_mrr, level_size, level_precision)
        print(precision)
        print('mrr:{} precision:\n{}'.format(mrr/size, '\n'.join(['{:.5f}'.format(p/size) for p in precision])))
        print("-----------------------")
        count = 0
        for x in precision:
            count+=x
            print(count/size)    
        
        if with_levels:
            for level in metrics_by_level:
                mrr, size, precision = metrics_by_level[level]
                print('level:{} mrr:{} precision:\n{}'.format(level, mrr/size, '\n'.join(['{:.5f}'.format(p/size)for p in precision])))
                count = 0
                print("-----------------------")
                for x in precision:
                    count+=x
                    print('{:.5f}'.format(count/size))
                print("-----------------------")
    
    def run_evaluation_all_results(self):
        attribute_map = self._get_default_attributes()

        json_file = self.config.evaluation[self.config.default_evaluation_index]['query_file']
        evaluation_items = {}
        with open(json_file) as f:
            data = json.load(f)    
            for i, item in enumerate(data):
                print(i)
                evaluation_item = EvaluationItem()
                evaluation_item.build_from_json(item)
                evaluation_items[item['id']] = evaluation_item
        print(evaluation_items.keys(), json_file)
        result_zero_file = self.config.evaluation[self.config.default_evaluation_index]['result_zero_file']
        eval_zero_items = {}
        not_found_list = []
        with open(result_zero_file) as f:
            
            for i, item in enumerate(f):
                not_found = True    
                result = json.loads(item)
                #print(result.keys())
                count = 0
                for j, n_item in enumerate(result['query_matches']):
                    json_item = EvaluationItem()
                    json_item.build_from_nalir_json({'query_matches': n_item})
                    
                    for k,qm in enumerate(json_item.query_match):
                        if qm.has_default_mapping() and qm.table in attribute_map:
                            json_item.query_match[k].replace_default_mapping(attribute_map[qm.table])

                    # if result['query_id'] == 40 and count < 20:
                    #     print(evaluation_items[result['query_id']].query_match, "---", json_item.query_match)
                    #     count+= 1
                    #print(result.keys())
                    #print(evaluation_items.keys())
                    if json_item == evaluation_items[result['query_id']]:
                        print(j)
                        result['query_candidate_item'] = j
                        eval_zero_items[result['query_id']] = result
                        not_found = False
                        break
                
                if not_found:
                    not_found_list += [result]
        print("xxx", [x['query_id'] for x in not_found_list])
        
        with open(result_zero_file,"w") as f:
            for item in [eval_zero_items[key] for key in eval_zero_items] + not_found_list:
                f.write("{}\n".format(json.dumps(item)))
            
        result_file = self.config.evaluation[self.config.default_evaluation_index]['result_file']
        result_data = []
        with open(result_file) as f:
            for line in f:
                item = json.loads(line)
                if item['query_id'] in eval_zero_items:
                    result_data += [eval_zero_items[item['query_id']]]
                else:
                    result_data += [item]
        #print(eval_zero_items.keys())
        #sys.exit()
        with open(result_file, 'w') as f:
            for item in result_data:
                f.write('{}\n'.format(json.dumps(item)))
        


    #merge previous result with new result
    def re_run_evaluation(self):
        attribute_map = self._get_default_attributes()
        json_file = self.config.evaluation[self.config.default_evaluation_index]['query_file']
        evaluation_items = []
        with open(json_file) as f:
            data = json.load(f)    
            for i, item in enumerate(data):
                evaluation_item = EvaluationItem()
                evaluation_item.build_from_json(item)
                evaluation_items += [evaluation_item]

        result_file = self.config.evaluation[self.config.default_evaluation_index]['result_file']
        result_data = []
        count = {'bigger': 0, 'lower': 0, 'improve': 0}
        with open(result_file) as f:
            for line in f:
                found_item = -1
                all_found = False
                result = json.loads(line)
                idx = result['query_id'] - 1
                found_item = 0
                for j, n_item in enumerate(result['query_matches']):
                    json_item = EvaluationItem()
                    json_item.build_from_nalir_json({'query_matches': n_item})
                    
                    # for k,qm in enumerate(json_item.query_match):
                    #     if qm.has_default_mapping() and qm.table in attribute_map:
                    #         json_item.query_match[k].replace_default_mapping(attribute_map[qm.table])
                     
                    if evaluation_items[idx] == json_item and (j < result['query_candidate_item'] or result['query_candidate_item'] == -1):
                        print("query_id : {} old: {}, new: {}".format( result['query_id'], result['query_candidate_item'],j))
                        if j >  result['query_candidate_item']:
                            if result['query_candidate_item'] == -1: 
                                count['improve'] += 1
                            else:
                                count['bigger'] += 1
                        else:
                            count['lower']+=1
                        found_item = idx
                        break

                    if result['query_id'] == 13:
                        print (j, json_item == evaluation_items[idx])
                if found_item != -1:
                    result['query_candidate_item'] = found_item
                        
                result_data += [result]
        print(count)
                        

        # with  open(result_file, 'w') as f:
        #     for item in result_data:
        #         f.write('{}\n'.format(json.dumps(item)))



    def run_nalir_k2d_evaluation(self, result_file, output_file, with_levels=True):
        items = []
        ids = []
        with open(result_file, 'r') as f:
            for line in f:
                evaluated_item = {}
                item = json.loads(line)
                print("getting {} list has {} values".format(item["id"], len(self.evaluation_items)))
                evaluation_item = self.evaluation_items[item['id'] - 1]
                print(item['segments'], evaluation_item.segments)
                evaluated_item['query_id'] = item['id']
                found_qm = False
                
                for i, qmatch in enumerate(item['query_matches']):
                    json_item = EvaluationItem()
                    json_item.build_from_nalir_json({'query_matches': qmatch})
                    evaluated_item['query_matches'] = item['query_matches']
                    if evaluation_item.has_words_equal(json_item):
                        evaluated_item['query_candidate_item'] = i 
                        found_qm = True
                        break

                if not found_qm:
                    evaluated_item['query_candidate_item'] = -1
                if with_levels:
                    evaluated_item['level'] = item['level']
                ids+= [item["id"]]
                items += [evaluated_item]
        
        with open(output_file, 'w') as f:
            for item in items:
                f.write("{}\n".format(json.dumps(item)))
        

        fdata =  open(output_file, 'a+')
        print(self.config.evaluation[self.config.default_evaluation_index]['result_file'])
        with open(self.config.evaluation[self.config.default_evaluation_index]['result_file'],'r') as f:
            for line in f:
                if not json.loads(line)["query_id"] in ids:
                    fdata.write(line)
        fdata.close()
                
                




        
        