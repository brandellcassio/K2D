import sys
sys.path.append('../')

import string
from utils import ConfigHandler, Similarity, stopwords
from index import IndexHandler
from query_match import QueryMatchHandler
from keyword_match import KeywordMatchHandler

config = ConfigHandler()
indexHandler = IndexHandler()
indexHandler.load_indexes(config.value_index_filename, config.schema_index_filename)
similarity = Similarity(indexHandler.schema_index)

#sc = SparkContext( 'local', 'pyspark')

#segments = ["checkin" , "Los Angeles", "Morrocan", "restaurant"]
#segments = ['rivers', 'length', '1120.0', '1579.0']
#segments = ['city', 'canada', 'population']
#segments = ['africa', 'asia']
#segments =  ["portugal", "capital"]
#segments = ["information retrieval", "papers", "conference", "vldb", "keyword"]
#segments = ["organizations", "north america"]
#segments = ['portugal', 'papers', 'conference', 'vldb']
#segments= ['tips', '2012', 'user', 'review']
#segments = ['Madison', 'escape games']
#segments =  ["Dallas","rating","Italian","restaurant"]
#segments =[]
#segments = ["Madison", "Italian", "neighborhoods", "restaurants"]
#segments = ["homepage", "conference", "vldb"]
segments = ["people", "Texas do Brazil"]
query = [w.lower().strip(string.punctuation) for s in segments for w in s.replace('"', '').split(' ') if w not in stopwords()]
kwHandler = KeywordMatchHandler(similarity)
print("Generating schema matches")
skm_matches = kwHandler.schema_keyword_match_generator(query, indexHandler.schema_index, threshold=config.similarity_threshold)
skm_matches = kwHandler.filter_kwmatches_by_segments(skm_matches, segments)

print("skm: ", skm_matches)
print("Generating values matches")
vkm_matches = kwHandler.value_keyword_match_generator(query, indexHandler.value_index)
print("Filter by segments")
vkm_matches = kwHandler.filter_kwmatches_by_segments(vkm_matches, segments)
print("vkm: ", vkm_matches)
qm_handler = QueryMatchHandler()
all_matches = vkm_matches 

print("Generating Query Matches")
print(len(skm_matches | vkm_matches))
#qm_handler.generate_query_matches(query, skm_matches | vkm_matches, segments=segments)
qm_handler.parallel_generate_query_matches(query, skm_matches | vkm_matches, segments=segments)
#exit()
print("Ranking query matches")
#qm_handler.rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity)
qm_handler.parallel_rank_query_matches(indexHandler.value_index, indexHandler.schema_index, similarity)
for i, query_match in enumerate(qm_handler.query_matches):
    print(i, query_match, query_match.total_score)
    if i == 10:
        break