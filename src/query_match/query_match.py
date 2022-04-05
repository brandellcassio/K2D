from random import sample
import re
import json
from utils import get_logger
from math import log
from utils import ConfigHandler
from nltk.stem import WordNetLemmatizer
from utils import calcuate_index_weight


'''
Query match is a set of tuple-sets that, if properly joined,
can produce networks of tuples that fulfill the query. They
can be thought as the leaves of a Candidate Network.
'''

logger = get_logger(__name__)
class QueryMatch:

    def __init__(self, matches):
        self.matches = set(matches)
        self.value_score = 1.0
        self.schema_score = 1.0
        self.total_score = 1.0
        self.tables_on_match = set()
        self.config = ConfigHandler()
        self.word_lemmatize = WordNetLemmatizer()
        self.attribute_map = None
        self._load_attributes()

    def get_random_keyword_match(self):
        return sample(self.matches,k=1)[0]

    def _load_attributes(self):
        with open(self.config.relations_file, "r") as f:
            self.attribute_map = {item['name']:attr['name']  for item in json.load(f) \
                for attr in item['attributes'] if attr.get('importance','') == 'primary' }

    def calculate_total_score(self, value_index, schema_index, similarity, log_score=False):

        has_value_terms = self.calculate_value_score(value_index, schema_index)
        has_schema_terms = self.calculate_schema_score(similarity, schema_index)

        if has_value_terms:
            self.total_score += self.value_score

        if has_schema_terms:
            self.total_score += self.schema_score
        
        self.total_score -= log(len(self))
        
        logger.info("scores for : {} value_score: {} schema_score: {} total: {}".format(self.matches,
        self.value_score, 
        self.schema_score, 
        self.total_score))
       
    def calculate_schema_score(self,similarity, schema_index, split_text=True):
        #all_norms = schema_index['collection']['_all_']['norms']
        
        has_schema_terms = False
        for keyword_match in self.matches:

            count = 0
            for table, attribute, schema_words in keyword_match.schema_mappings():
                has_schema_terms = True
                
                logger.debug('for {0}.{1}'.format(table, attribute))

                schemasum = 0

                pattern = re.compile('[_,-]')
                attributes =  [attribute] if not split_text else pattern.split(attribute)
                tables = [table] if not split_text else pattern.split(table)
                inner_count = 0
                
                #print(table, attr)
                #constant = schema_index[table][attr]['avg_weights'][0]/schema_index[table][attr]['norms'][0]
                for term in schema_words:
                    inner_count +=1
                    for table_item in tables:
                        max_sim = 0
                        for attribute_item in attributes:
                            sim = similarity.word_similarity(term,table_item,attribute_item, get_average=self.config.similarity_average)
                            #print('similarity btw {0}: {1}.{2} {3}'.format(term, table_item, attribute_item, sim))
                            if sim > max_sim:
                                max_sim = sim
                    schemasum+=max_sim
                            
                
             
                
                #max_sim *= constant
                
                #schemasum += max_sim
                   
                #schemasum /= inner_count
                #print("s", schema_words,table, attribute, schemasum,log(self.config.normalization_value*schemasum))
                self.schema_score +=self.config.normalization_value*log(schemasum)

        return has_schema_terms

    def calculate_value_score(self, value_index, schema_index):
        has_value_terms = False
        #print("---------------------")
        for keyword_match in self.matches:
            for table, attribute, keywords in keyword_match.value_mappings():
                has_value_terms = True
                norm = schema_index[table][attribute]['norms'][self.config.selected_norm]
                max_frequency  = schema_index[table][attribute]['max_frequency']

                wsum = 0.0
                for term in keywords:
                    frequency = value_index.get_frequency(term,table,attribute)
                    tf = calcuate_index_weight(frequency, max_frequency, self.config.selected_norm)
                    iaf = value_index.get_iaf(term, index=self.config.selected_norm) 
                    wsum = wsum + tf*iaf
                    #print("v", term, table, attribute, (tf*iaf)/norm)

                cos = wsum / norm
                # if 'texas' in keywords:
                #     print("v", table, attribute, log(cos), log((1-self.config.normalization_value) * cos), cos)
                self.value_score += (1-self.config.normalization_value)*log(cos)
                #print(cos, self.value_score)
        #print("---------------------")
        


        return has_value_terms

    def __iter__(self):
        return iter(self.matches)

    def __len__(self):
        return len(self.matches)

    def __repr__(self):
        return repr(self.matches)

    def __str__(self):
        return repr(self.matches)

    #TODO Fazer métodos de import/export para json
    def to_json(self):
        return '[{}]'.format(','.join([x.to_json() for x in self.matches]))
