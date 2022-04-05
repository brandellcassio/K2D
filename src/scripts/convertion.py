import os
import sys
import json

def get_segments(query):
    tokens = query.split("\"")
    segments = []
    for i, token in range(tokens):
        if i % 2 == 0:
            segments += tokens.split(" ")
        else:
            segments += [token]
    return segments


def mas_convert_gt_tsv_to_json(input_filename, output_filename, dt_type): 
    ground_truth = []
    query_data = []

    dataset = input_filename

    dt_params = {'mas': (1, -1), 'mondial': (3, 2), 'imdb':(4,2)}
    with open(dataset) as f:
        
        match_param, sql_param = dt_params[dt_type]
        for i,line in enumerate(f):
            if i == 0:
                continue

            line_segments = line.split('\n')[0].split('\t')
            
            result = line_segments[match_param].lower()
            candidates = result.split(';')
            generate_candidates = {}
            gt_item = {}
            gt_item['id'] = i
            gt_item['natural_language_query'] = line_segments[0]
            gt_item['sql_query'] = "" if sql_param == -1 else line_segments[sql_param] 
            
            query_matches_by_table = {}
            keywords = []
            #print(candidates)
            for candidate in candidates:
                candidate_tokens = candidate.split(':')
                segment = candidate_tokens[0].lstrip().replace('.', '')
                table, attr = candidate_tokens[1].split('.')
                #print(candidate_tokens)
                type_word = candidate_tokens[2]
                data = query_matches_by_table.setdefault(table, {})
                #print("word: ", segment ,"type: ", type_word, type_word == 'nt')
                filter_type = 'schema_filter' if type_word == 'nt' else 'value_filter'
                dict_to_fill = data.setdefault(filter_type, {}) 
                dict_to_fill.setdefault(attr, []).append(segment)
                keywords += [segment]
                
            gt_item['segments'] = keywords
            query_matches = []
            
            for table in query_matches_by_table:
                query_match_item = {}
                query_match_item['table'] = table
                for mapping_type  in ['schema_filter', 'value_filter']:
                    query_match_item[mapping_type] = []

                    for attr in query_matches_by_table[table].setdefault(mapping_type, []):
                        attr_item = {}
                        attr_item['attribute'] = attr
                        attr_item['keywords'] = [y for w in query_matches_by_table[table][mapping_type][attr] for y in w.split(" ")]
                        query_match_item[mapping_type] += [attr_item]
                
                query_matches += [query_match_item]
            gt_item['query_matches'] = query_matches
            ground_truth = ground_truth + [gt_item]
            
    with open(output_filename, 'w') as f:
        json.dump(ground_truth, f, indent=4)


def yelp_jsonl_to_json(input_filename, output_filename):
    ground_truth = []
    query_data = []

    dataset = input_filename
    with open(dataset) as f:
        for i, line in enumerate(f):
            item = json.loads(line)
            query_item = {}
            query_item['query_id'] = i+1
            query_item['query'] = item['query']
            query_item['segments'] = ['"{}"'.format(kw) for kw in item['mapping'] if ' ' in item['mapping']] +\
            [kw for kw in item['mapping'] if not ' ' in item['mapping'] and not '#' in tem['mapping']]
            
            query_matches_by_table = {}
            for kw in item['mapping']:
                kw_mapping, type_word = item['mapping'][kw]
                table, attr = kw_mapping.split('.')
               
                type_word = type_word.lower()
                data = query_matches_by_table.setdefault(table, {})
                #print("word: ", segment ,"type: ", type_word, type_word == 'nt')
                filter_type = 'schema_filter' if type_word == 'nt' else 'value_filter'
                dict_to_fill = data.setdefault(filter_type, {}) 
                dict_to_fill.setdefault(attr, []).append(kw)

            query_matches = []
            for table in query_matches_by_table:
                query_match_item = {}
                query_match_item['table'] = table
                for mapping_type  in ['schema_filter', 'value_filter']:
                    query_match_item[mapping_type] = []

                    for attr in query_matches_by_table[table].setdefault(mapping_type, []):
                        attr_item = {}
                        attr_item['attribute'] = attr
                        attr_item['keywords'] = [y for w in query_matches_by_table[table][mapping_type][attr] for y in w.split(" ")]
                        query_match_item[mapping_type] += [attr_item]
                
                query_matches += [query_match_item] 
            query_item['query_matches'] = query_matches
            query_data+= [query_item]
    
    with open(output_filename,'w') as f:
        json.dump(query_data, f, indent=4)

if __name__ == "__main__":
    yelp_jsonl_to_json(sys.argv[1], sys.argv[2])
    