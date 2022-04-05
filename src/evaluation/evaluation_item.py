from keyword_match import KeywordMatch
class EvaluationItem:
    
    def __init__(self):
        self.natural_language_query = ''
        self.sql_language_query = ''
        self.segments = []
        self.query_match = set()
        self.ranked_matches = []
        self.qm_position = -1
        self.id = -1 
        
    def build_from_json(self, json_data):
        #self.natural_language_query = json_data['natural_language_query']
        #self.sql_language_query = json_data['sql_query']
        self.segments = json_data['segments']
        key = 'id'
        if not key in json_data:
            key = 'query_id'
        self.id = json_data[key]
        
        for raw_kw_match in json_data['query_matches']:
            kw_match = KeywordMatch.from_json_serializable(raw_kw_match)
            self.query_match.add(kw_match)
    
    def build_from_nalir_json(self, json_data):
        #self.segments = json_data['segments']
        #self.id = json_data['id']
        
        for raw_kw_match in json_data['query_matches']:
            kw_match = KeywordMatch.from_json_serializable(raw_kw_match)
            self.query_match.add(kw_match)
        


    def find_query_match_gt(self):
        gt_mapping = set([y for x in self.query_match for y in x.kw_mappings()])
        for i, ranked_query_match in enumerate(self.ranked_matches):
            result_mapping = set([y for x in ranked_query_match for y in x.kw_mappings()])
            is_equal = True

            if (gt_mapping & result_mapping) == gt_mapping:          
                self.qm_position = i
                break

            #print(i, gt_mapping - result_mapping)
        
        #print(self.qm_position, self.query_match, list(self.ranked_matches)[:5])
    
    def __eq__(self, other):
        result = isinstance(other,EvaluationItem)
        if not result:
            return result

        result_mapping = [y for x in other.query_match for y in x.kw_mappings()]
        gt_mapping = [y for x in self.query_match for y in x.kw_mappings()]

        if len(result_mapping) != len(gt_mapping):
            return False 

        for i, item in enumerate(result_mapping):
            if not item in gt_mapping:
                return False
        return True

    def has_words_equal(self, other):
        result_mapping = [y for x in other.query_match for y in x.kw_mappings()]
        gt_mapping = [y for x in self.query_match for y in x.kw_mappings()]

        for i, item in enumerate(gt_mapping):
            if not item in result_mapping:
                return False

        return True

        

        
        