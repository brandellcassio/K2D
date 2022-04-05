import  itertools
import copy
import time
from collections import Counter
from itertools import combinations
from utils import parallel_calc_metrics, partition_list_by_cpu
from utils import ConfigHandler,get_logger
from utils import combinations_in_range,combinadic,choose,combination_partitions
from keyword_match import KeywordMatch
from index import BabelHash
from .query_match import QueryMatch

import math
import multiprocessing
from multiprocessing import Process

logger = get_logger(__name__)


class QueryMatchHandler:

    def __init__(self):
        self.query_matches = []

        

    def generate_query_matches(self, keyword_query, keyword_matches, **kwargs):
        #Input:  A keyword query Q, The set of non-empty non-free tuple-sets Rq
        #Output: The set Mq of query matches for Q
        max_qm_size = kwargs.get('max_qm_size',5)
        segments = kwargs.get('segments', [])
        self.query_matches = []
        set_query_match = set()
        
        # len(segments)+1
        # min(len(segments), max_qm_size)+1
        for i in range(1, len(segments)+1):
            print("combining ",i)
            count=0
            for candidate_match in itertools.combinations(keyword_matches,i):
                count+=1
                #print("candidate query match: {}".format(candidate_match))
                if self.has_minimal_cover(candidate_match,keyword_query) and \
                    self.has_single_schema_filter(candidate_match, keyword_query):
                #and \
                #(len(sements) != 0  and self.check_segments(segments, candidate_match):
                    merged_query_match = self.merge_schema_filters(candidate_match)

                    query_match = QueryMatch(merged_query_match)

                    #TODO: checking user group
                    self.query_matches.append(query_match)
                    set_query_match.add(query_match)
            #print(len(self.query_matches), count)
    

    def has_minimal_cover(self, candidate_match, keyword_query):
        #Input:  A subset CM (Candidate Query Match) to be checked as total and minimal cover
        #Output: If the match candidate is a TOTAL and MINIMAL cover

        total_counter = Counter(keyword
                                  for keyword_match in candidate_match
                                  for keyword in keyword_match.keywords())
       
        if len(total_counter)!=len(keyword_query):
            return False


        # Check whether it is minimal
        for keyword_match in candidate_match:
            local_counter = Counter(keyword_match.keywords())
            # subtract operation keeps only positive counts
            if len(total_counter-local_counter)==len(keyword_query):
                return False


        return True

    def has_single_schema_filter(self, candidate_match, keyword_query):
        return Counter([y[0] 
        for x in candidate_match 
        for y in x.schema_filter ]).most_common(1)[0][1] == 1

    def merge_schema_filters(self, query_matches):
        table_hash={}
        for keyword_match in query_matches:
            joint_schema_filter,value_keyword_matches = table_hash.setdefault(keyword_match.table,({},set()))

            for attribute, keywords in keyword_match.schema_filter:
                joint_schema_filter.setdefault(attribute,set()).update(keywords)

            if len(keyword_match.value_filter) > 0:
                value_keyword_matches.add(keyword_match)

        merged_qm = set()
        for table,(joint_schema_filter,value_keyword_matches) in table_hash.items():
            if len(value_keyword_matches) > 0:
                joint_value_filter = {attribute:keywords
                                    for attribute,keywords in value_keyword_matches.pop().value_filter}
            else:
                joint_value_filter={}

            joint_keyword_match = KeywordMatch(table,
                                            value_filter=joint_value_filter,
                                            schema_filter=joint_schema_filter)

            merged_qm.add(joint_keyword_match)
            merged_qm.update(value_keyword_matches)

        return merged_qm

    def rank_query_matches(self, value_index, schema_index, similarity, log_score=False):
        print("len {}".format(len(self.query_matches)))
        current_ratio = 0
        for i, query_match in enumerate(self.query_matches):
            if int(((i+1)/len(self.query_matches)) * 10) > current_ratio:
                current_ratio = int(((i+1)/len(self.query_matches)) * 10) 
                #print("percent: {}".format(current_ratio/10))

            query_match.calculate_total_score(value_index,schema_index, similarity)

        self.query_matches.sort(key=lambda query_match: query_match.total_score,reverse=True)
    
    def get_keyword_match_score(self, value_index, schema_index, similarity, kw_match, log_score=False):
        query_match = QueryMatch(set([kw_match]))
        query_match.calculate_total_score(value_index,schema_index, similarity, log_score)
        return query_match.total_score


    def _calc_query_match_score(self, objects, query_matches, sizes):
        for i in range(sizes[0], sizes[1]):
            item = query_matches[i]
            item.calculate_total_score(objects['value_index'],
            objects['schema_index'],objects['similarity'])
            query_matches[i] = item

    def parallel_rank_query_matches(self, value_index, schema_index, similarity):
        
        if len(self.query_matches) < 10:
            return self.rank_query_matches(value_index,schema_index,similarity)
        
        with multiprocessing.Manager() as manager:
            shrd_query_match = manager.list(self.query_matches)
            shrd_objects = manager.dict()
            shrd_objects['value_index'] = value_index
            shrd_objects['schema_index'] = schema_index
            shrd_objects['similarity'] = similarity
            num_process, sizes = partition_list_by_cpu(self.query_matches)
            processes = [Process(target=self._calc_query_match_score, args=(shrd_objects, shrd_query_match, sizes[i])) for i in range(num_process)]
            
            for p in processes:
                p.start()

            for p in processes:
                p.join()

            self.query_matches = shrd_query_match[:]
          
        self.query_matches.sort(key=lambda query_match: query_match.total_score,reverse=True)

    
    def _generate_query_matches(self, size,keywords, partitions,keyword_matches, query_matches):
        
        for i in range(size[0], size[1]):
            if i >= len(partitions):
                continue

            partition = partitions[i]
            results = [ QueryMatch(self.merge_schema_filters(kw_match)) 
                for kw_match in combinations_in_range(keyword_matches,partition[0],partition[1],partition[2]) 
                if self.has_minimal_cover(kw_match,keywords)]
            query_matches += results


    def parallel_generate_query_matches(self,keywords,keyword_matches, **kwargs):
        segments = kwargs.get('segments', [])
        max_qm_size = kwargs.get('max_qm_size',3)
        num_proceses  = multiprocessing.cpu_count()
        partitions = combination_partitions(num_proceses,keyword_matches,len(segments))

        if len(partitions) < 2*num_proceses:
            self._generate_query_matches((0,len(partitions)), keywords, partitions, keyword_matches, self.query_matches)
            return

        with multiprocessing.Manager() as manager:
            shrd_keyword_match = manager.list(keyword_matches)
            shrd_partitions = manager.list(partitions)
            shrd_query_matches = manager.list([])
            shrd_keywords = manager.list(keywords)
            num_process, sizes = partition_list_by_cpu(partitions)

            #print(sizes, partitions)
            processes = [Process(target=self._generate_query_matches, 
                args=(sizes[i], shrd_keywords, shrd_partitions, shrd_keyword_match, shrd_query_matches)) 
                for i in range(num_process) if sizes[i][0] != -1 and sizes[i][0] < len(partitions)]  
            
            for p in processes:
                p.start()

            for p in processes:
                p.join()

            self.query_matches = shrd_query_matches[:]
