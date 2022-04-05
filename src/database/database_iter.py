import string
import sys
import re
import psycopg2
from psycopg2 import sql

from utils import ConfigHandler, get_logger, stopwords
from unidecode import unidecode
from nltk.stem import WordNetLemmatizer

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self, database_table_columns, **kwargs):
        self.database_table_columns=database_table_columns

        self.limit_per_table = kwargs.get('limit_per_table', None)

        self.config = ConfigHandler()
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

        self.table_hash = self._get_indexable_schema_elements()
        self.stop_words = stopwords()
        
        self.tokenizer = re.compile("\\W+|_|`")
        self.url_match = re.compile("^(https?://)?\\w+\.[\\w+\./\~\?\&]+$")
        self.word_lemmatize = WordNetLemmatizer()

    def _tokenize(self,text):
        tokenized = [word_part.encode('ascii', 'ignore').decode() for word in text.lower().split() 
        for word_part in self.tokenizer.split(word.strip(string.punctuation))
        if word_part not in self.stop_words]
        
        tokens = set()
        for w in tokenized:  
            tokens.add(self.word_lemmatize.lemmatize(w, pos='n'))
            tokens.add(self.word_lemmatize.lemmatize(w, pos='v'))
            tokens.add(w)
        tokenized = list(tokens)

        return tokenized
    
    def _schema_element_validator(self,table,column):
        return True

    def _get_indexable_schema_elements(self):
         with psycopg2.connect(self.conn_string) as conn:
            with conn.cursor() as cur:
                GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL='''
                    SELECT c.table_name, c.column_name, 
                        t.constraint_type, c.udt_name 
                    FROM 
                    (SELECT kcu.column_name , kcu.table_name, tc.constraint_type FROM
                        information_schema.table_constraints tc JOIN 
                        information_schema.key_column_usage kcu ON
                        tc.table_name = kcu.table_name  and 
                        tc.constraint_name  = kcu.constraint_name
                    WHERE 
                        kcu.table_schema = 'modial' AND 
                        kcu.table_schema  = tc.table_schema) t RIGHT JOIN
                        information_schema.columns as c
                        ON c.column_name = t.column_name 
                        AND c.table_name = t.table_name
                    WHERE c.table_schema = 'mondial'
                '''
                cur.execute(GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL)
                table_hash = {}

                for table,column,constraint_type,data_type in cur.fetchall():
                    if table in self.config.remove_from_index or not (table,column) in self.database_table_columns\
                        or '{}.{}'.format(table,column) in self.config.remove_attributes:
                        #print("continuing for {}.{}".format(table, column))
                        continue

                    if column == '__search_id':
                        continue
                    
                    if (constraint_type == "PRIMARY KEY" and\
                        (data_type.startswith("int") or data_type.startswith("numeric"))) or '_id' in column:
                        continue        
                    table_hash.setdefault(table,{}).update({column: data_type})
                print(table_hash)
                return table_hash

    def __iter__(self):
        for table,columns in self.table_hash.items():
            indexable_columns = [col for col in columns if self._schema_element_validator(table,col)]
            #print(indexable_columns)
            if len(indexable_columns)==0:
                continue

            print('\nINDEXING {}({})'.format(table,', '.join(indexable_columns)))

            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    '''
                    NOTE: Table and columns can't be directly passed as parameters.
                    Thus, the sql.SQL command with sql.Identifiers is used
                    '''

                    if self.limit_per_table is not None:
                        text = (f"SELECT ctid, {{}} FROM {{}} LIMIT {self.limit_per_table};")
                    else:
                        text = ("SELECT ctid, {} FROM mondial.{};")
                    #print(text)
                    cur.execute(
                        sql.SQL(text)
                        .format(sql.SQL(', ').join(sql.Identifier(col) for col in indexable_columns),
                                sql.Identifier(table))
                            )

                    arraysize = 100000
                    while True:
                        results = cur.fetchmany(arraysize)
                        if not results:
                            break
                        for row in results:
                            ctid = row[0]
                            for col in range(1,len(row)):
                                column = cur.description[col][0]
                                if row[col] == None:
                                    continue
                                is_number = self.table_hash[table][column].startswith('float')
                                tokens = self._tokenize(unidecode(str(row[col]))) if not is_number else [str(row[col])]
                                for word in tokens:
                                    yield table,ctid,column, word
