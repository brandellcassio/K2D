import re
import sys
import psycopg2
from psycopg2 import sql
import string
from utils import ConfigHandler, get_logger

logger = get_logger(__name__)
class DatabaseIter:
    def __init__(self, showLog=True):
        self.config = ConfigHandler()

        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(\
            self.config.connection['host'], self.config.connection['database'], \
            self.config.connection['user'], self.config.connection['password'])

        self.table_hash = self._get_indexable_schema_elements()
        self.showLog = showLog
        self.stop_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you',
                           "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself',
                           'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her',
                           'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them',
                           'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom',
                           'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are',
                           'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                           'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and',
                           'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at',
                           'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
                           'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up',
                           'down','in', 'out', 'on', 'off', 'over', 'under', 'again', 'further',
                           'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all',
                           'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such',
                           'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
                           's', 't', 'can', 'just', 'don', "don't", 'should', "should've", 'now',
                           'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
                           "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't",
                           'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn',
                           "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't",
                           'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won',
                           "won't", 'wouldn', "wouldn't"]
        self.tokenizer = re.compile("\\W+")
        self.url_match = re.compile(("^(https?://)?\\w+\.[\\w+\./\~\?\&]+$")

    def _tokenize(self,text):
        if self.url_match.search(text) is not None:
            return ([text], True)
            
        tokenized = [self.tokenizer.split(word.strip(string.punctuation))
                for word in text.lower().split()
                if word not in self.stop_words]
        
        check_tokenized = []
        for word in tokenized:
            if len([ch for ch in word if ch in string.punctuation]) != 0:
                check_tokenized += self.tokenizer.split(word)
            else:
                check_tokenized += [word]
                
        return ([token for token in check_tokenized if token != ''], False)

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
                        kcu.table_schema = 'public' AND 
                        kcu.table_schema  = tc.table_schema) t RIGHT JOIN
                        information_schema.columns as c
                        ON c.column_name = t.column_name 
                        AND c.table_name = t.table_name
                    WHERE c.table_schema = 'public'
                '''
                cur.execute(GET_TABLE_AND_COLUMNS_WITHOUT_FOREIGN_KEYS_SQL)
                table_hash = {}
                for table,column,constraint_type,data_type in cur.fetchall():
                    if table in self.config.remove_from_index:
                        continue

                    if column == '__search_id':
                        continue
                    
                    if constraint_type == "PRIMARY KEY" and data_type.startswith("int"):
                        continue

                    table_hash.setdefault(table,[]).append(column)
                return table_hash

    def __iter__(self):
        for table,columns in self.table_hash.items():

            indexable_columns = [col for col in columns if self._schema_element_validator(table,col)]

            if len(indexable_columns)==0:
                continue

            print('\nINDEXING {}({})'.format(table,', '.join(indexable_columns)))

            with psycopg2.connect(self.conn_string) as conn:
                with conn.cursor() as cur:
                    '''
                    NOTE: Table and columns can't be directly passed as parameters.
                    Thus, the sql.SQL command with sql.Identifiers is used
                    '''
                    cur.execute(
                        sql.SQL("SELECT ctid, {} FROM {};")
                        .format(sql.SQL(', ').join(sql.Identifier(col) for col in indexable_columns),
                                sql.Identifier(table))
                            )
                    data = cur.fetchmany(1000)
                    while len(data) != 0:
                        for row in data:
                            ctid = row[0]
                            for col in range(1,len(row)):
                                column = cur.description[col][0]
                                (is_url, tokens) = self._tokenize( str(row[col]) )
                                for word in tokens:
                                    if len([ch for ch in word if ch in string.punctuation]) != 0 and not is_url:
                                        print("Tokenizer not working {}".format(word, row[col]))
                                        sys.exit()
                                    yield table,ctid,column, word
                        data = cur.fetchmany(1000)
