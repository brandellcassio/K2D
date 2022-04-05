import gc
from glob import glob
import shelve
from contextlib import ExitStack
import json
from pprint import pprint as pp
from math import log1p, pow, sqrt

from utils import ConfigHandler,get_logger,memory_size,calcuate_index_weight, calcuate_inverse_frequency
from database import DatabaseHandler

from .value_index import ValueIndex
from .schema_index import SchemaIndex
from .schema_graph import SchemaGraph
from .babel_hash import BabelHash
import sys
logger = get_logger(__name__)

class IndexHandler:
    def __init__(self):
        self.config = ConfigHandler()
        self.value_index = ValueIndex()
        self.schema_index = SchemaIndex()
        self.database_handler = DatabaseHandler()
        self.partial_index_count=0

    

    def create_indexes(self):
        self.create_partial_schema_index()
        self.create_partial_value_indexes()
        self.merge_partial_value_indexes_and_process_max_frequency()
        self.process_norms()

    def create_partial_schema_index(self):
        print(f'self.config.relations_file {self.config.relations_file}')
        tables_attributes = None

        if self.config.relations_file is not None:
            print('Get table_attributes from relations_file')
            with open(self.config.relations_file,'r') as relations_file:
                tables_attributes = [(entry['name'],attribute_entry['name'])
                for entry in json.load(relations_file)
                for attribute_entry in entry['attributes'] if entry['type'] != 'relationship' \
                and(attribute_entry['type'] != 'fk' and attribute_entry['type'] != 'pk')
                ]
                
                print(tables_attributes)
        else:
            print('Get table_attributes from database')
            tables_attributes = self.database_handler.get_tables_and_attributes()

        print("x: ", tables_attributes)
        self.schema_index.create_entries(tables_attributes)
        print(f'num attr: {self.schema_index.get_num_total_attributes()}')
        print(f'ATTRIBUTES:')
        pp(self.schema_index.tables_attributes())

    def create_partial_value_indexes(self,**kwargs):
        
        if not self.config.create_index:
            return

        unit = kwargs.get('unit', 2**30)
        max_memory_allowed = kwargs.get('max_memory_allowed', 8)
        max_memory_allowed *= unit
        gb = 1 * unit
        babel = BabelHash.babel

        def part_index():
            self.partial_index_count+=1
            partial_index_filename = f'{self.config.value_index_filename}.part{self.partial_index_count:02d}'
            logger.info(f'Storing value_index.part{self.partial_index_count} in {partial_index_filename}')
            self.value_index.persist_to_shelve(partial_index_filename)
            self.value_index = ValueIndex()
            gc.collect()
            logger.info('Indexing operation resumed.')


        for table,ctid,attribute, word in self.database_handler.iterate_over_keywords(self.schema_index):
            #i+=1
            #print(table, attribute, word)
            if memory_size() >= max_memory_allowed:
                logger.info(f'Memory usage exceeded the maximum memory allowed (above {max_memory_allowed/gb:.2f}GB).')
                part_index()
            #if word == 'banjarmasin':
            #    print(table, attribute, ctid)
            self.value_index.add_mapping(word,table,attribute,ctid)
            
        part_index()

    def merge_partial_value_indexes_and_process_max_frequency(self):
        '''
        Para garantir que todos os arquivos abertos serão fechados, é bom utilizar
        a cláusula with. Mas como eu quero abrir um numero n de arquivos, eu pensei
        em utilizar essa classe ExitStack.

        Ao todo utilizaremos n arquivos parciais e um arquivo final/completo
         é para poder utilizar vários arquivos
        dentro do with. Dessa forma, eu garanto que todos eles serao fechados.
        '''

        num_total_attributes=self.schema_index.get_num_total_attributes()
        babel = BabelHash.babel

        filenames=glob(f'{self.config.value_index_filename}.part*')
        with ExitStack() as stack:
            partial_value_indexes = [stack.enter_context(shelve.open(fname,flag='r')) for fname in filenames]
            final_value_index = stack.enter_context(shelve.open(f'{self.config.value_index_filename}'))

            for partial_value_index in partial_value_indexes:
                babel.update(partial_value_index['__babel__'])         
            final_value_index['__babel__'] = babel

            for i in range(len(partial_value_indexes)):
                print(f'i {i} {filenames[i]}')
                for word in partial_value_indexes[i]:
                    #print("x: ", word)
                    if word in final_value_index or word == '__babel__':
                        continue

                    value_list = [ partial_value_indexes[i][word] ]

                    for j in range(i+1,len(partial_value_indexes)):
                        #print(f'  j {j} {filenames[j]}')
                        if word in partial_value_indexes[j]:
                            value_list.append(partial_value_indexes[j][word])

                    merged_babel_hash = BabelHash()
                    if len(value_list)>1:
                        for ( part_iaf ,part_babel_hash) in value_list:
                            for table in part_babel_hash:
                                merged_babel_hash.setdefault(table,BabelHash())
                                for attribute in part_babel_hash[table]:
                                    merged_babel_hash[table].setdefault( attribute , [] )
                                    merged_babel_hash[table][attribute]+= part_babel_hash[table][attribute]
                    else:
                        _,merged_babel_hash = value_list[0]
                    should_pass = False
                    for table in merged_babel_hash:
                        should_pass = True
                        for attribute in merged_babel_hash[table]:
                            #print(table, attribute, self.schema_index[table].keys())
                            frequency = len(merged_babel_hash[table][attribute])
                            max_frequency = self.schema_index[table][attribute]['max_frequency']
                            if frequency >= max_frequency:
                                self.schema_index[table][attribute]['max_frequency'] = frequency

                    if should_pass:
                        num_attributes_with_this_word = sum([len(merged_babel_hash[table]) for table in merged_babel_hash])
                        merged_iaf = log1p(num_total_attributes/num_attributes_with_this_word)
                        #merged_value = (merged_iaf,merged_babel_hash)
                        iafs = []
                        for j in range(self.config.num_metrics):
                            iafs += [calcuate_inverse_frequency(num_total_attributes, num_attributes_with_this_word, j)]
                        merged_value=(iafs,merged_babel_hash)
                        final_value_index[word]=merged_value

    def process_norms(self):
        with shelve.open(f'{self.config.value_index_filename}') as full_index:
            for word in full_index:
                if word == '__babel__':
                    continue

                iafs, babel_hash = full_index[word]
                print(word, iafs)
                for table in babel_hash:
                    for attribute in babel_hash[table]:
                        frequency = len(babel_hash[table][attribute])
                        max_frequency = self.schema_index[table][attribute]['max_frequency']
                        for i in range(self.config.num_metrics):
                            print(calcuate_index_weight(frequency,max_frequency,i), iafs[i], frequency, max_frequency)
                            self.schema_index[table][attribute]['norms'][i] += pow(calcuate_index_weight(frequency,max_frequency,i)*iafs[i], 2)
                        #pow(iaf*(frequency/max_frequency),2)

        for table in self.schema_index:
            for attribute in self.schema_index[table]:
                for i in range(len(self.schema_index[table][attribute]['norms'])):
                    self.schema_index[table][attribute]['norms'][i] = sqrt(self.schema_index[table][attribute]['norms'][i])
                
                print(table, attribute, self.schema_index[table][attribute]['norms'])
                
        
        self.schema_index.persist_to_shelve(self.config.schema_index_filename)

    def process_collection_database(self):   
        with shelve.open(f'{self.config.value_index_filename}') as full_index:
            for word in full_index:
                if word == '__babel__':
                    continue

                iafs, babel_hash = full_index[word]
                #print(word, iafs)
                count=[0.0,0.0,0.0,0.0]
                for table in babel_hash:
                    for attribute in babel_hash[table]:
                        frequency = len(babel_hash[table][attribute])
                        max_frequency = self.schema_index[table][attribute]['max_frequency']

                        for i in range(self.config.num_metrics):
                            self.schema_index[table][attribute]['avg_weights'][i]+=(calcuate_index_weight(frequency,max_frequency,i) * iafs[i])
                        
                        self.schema_index[table][attribute]['nterm'] += 1
                        #pow(iaf*(frequency/max_frequency),2)
            
        for table in self.schema_index:
            for attribute in self.schema_index[table]:
                if self.schema_index[table][attribute]['nterm'] != 0:
                    for i in range(len(self.schema_index[table][attribute]['avg_weights'])):
                        self.schema_index[table][attribute]['avg_weights'][i] /= self.schema_index[table][attribute]['nterm']
        
        self.schema_index.persist_to_shelve(self.config.schema_index_filename)

    def dump_indexes(self,value_index_filename,schema_index_filename):
        self.value_index.persist_to_shelve(value_index_filename)
        self.schema_index.persist_to_shelve(schema_index_filename)

    def load_indexes(self,value_index_filename,schema_index_filename,**kwargs):
        #self.value_index = self.value_index.load_from_shelve(value_index_filename,**kwargs)
        self.schema_index = self.schema_index.load_from_shelve(schema_index_filename)
        self.value_index.load_file(value_index_filename)
        
    def get_value_mappings(self, keyword):
        return self.value_index.get_mappings_from_file(keyword)

    def create_schema_graph(self):

        #TODO considerar custom fk constraints
        fk_constraints = self.database_handler.get_fk_constraints()
        schema_graph = SchemaGraph()
        for (constraint,
            table, column,
            foreign_table,foreign_column) in fk_constraints:

            schema_graph.add_fk_constraint(constraint,table,column,foreign_table,foreign_column)

        self.schema_graph = schema_graph
        print('SCHEMA CREATED')
