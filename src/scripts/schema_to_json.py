import sys
#reload(sys)
#sys.setdefaultencoding('utf8')

import sys
import string
import json
import re
import psycopg2
from psycopg2 import sql

# connector = mysql.connector.connect(user='root',\
#  password='', host='localhost',\
#  database='Mondial',charset="utf8")

conn_string="host='localhost' dbname='mondial_nalir' user='brandell' password=''"
query = "SELECT c.table_name, c.column_name, \
                        c.udt_name, t.constraint_type \
                    FROM \
                    (SELECT kcu.column_name , kcu.table_name, tc.constraint_type FROM\
                        information_schema.table_constraints tc JOIN \
                        information_schema.key_column_usage kcu ON\
                        tc.table_name = kcu.table_name  and \
                        tc.constraint_name  = kcu.constraint_name\
                    WHERE \
                        kcu.table_schema = 'public' AND \
                        kcu.table_schema  = tc.table_schema) t RIGHT JOIN\
                        information_schema.columns as c\
                        ON c.column_name = t.column_name\
                        AND c.table_name = t.table_name\
                    WHERE c.table_schema = 'public' order by c.table_name asc"

counter = 0
types = {
'varchar': 'text',\
'numeric' : 'number',\
'geocoord': 'text',\
'date':'text',\
'float8': 'number',\
'int8': 'number',
'timestamptz': 'timestamp'}

tables = []
table_item = {'name': '', 'attributes': [], 'type': 'entity'}
prev_table_name = ''
count_primary_keys = 0

relationship_attributes = {
	'country': 'country.code',\
	'country1': 'country.code',\
	'country2': 'country.code',\
	'organization':'organization.abbreviation',\
	'continent': 'continent.name',\
	'province': 'province.name',\
	'city' : 'city.name',\
	'river' : 'river.name',\
	'capital': 'city.name',\
	'mountain' : 'mountain.name',\
	'desert': 'desert.name',\
	'lake': 'lake.name',\
	'island' : 'island.name',\
	'sea': 'sea.name',\
	'sea1': 'sea.name',\
	'sea2': 'sea.name',\
}

relationships = []
with psycopg2.connect(conn_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)
        for (table_name,column, data_type, column_key) in cursor.fetchall():
            attribute_item = {}

            if prev_table_name != table_name and prev_table_name != '':
                #do something
                if count_primary_keys == len(table_item['attributes']):
                    for i, attr in enumerate(table_item['attributes']):
                        table_item['attributes'][i]['type'] = 'fk'
                    table_item['type'] = 'relationship'

                tables+= [table_item]
                prev_table_name = table_name
                table_item = {}
                table_item['name'] = table_name
                table_item['attributes'] = []
                count_primary_keys = 0

                table_item['type'] = 'entity'
            elif prev_table_name == '':
                prev_table_name = table_name
                table_item['name'] = table_name

            if column.lower() in relationship_attributes:
                print(table_name, column)
                relationship_item = {}
                origin_table, origin_item =relationship_attributes[column.lower()].split('.')
                if origin_table != table_name:
                    relationship_item['foreignAttribute'] = column
                    relationship_item['primaryAttribute'] = origin_item
                    relationship_item['foreignRelation'] = table_name
                    relationship_item['primaryRelation'] = origin_table
                    relationships += [relationship_item]
            attribute_item['name'] = column
            
            if column_key == 'PRIMARY KEY':
                attribute_item['type'] = 'pk'
                count_primary_keys +=1
            else:
                attribute_item['type'] = types[data_type]

            if table_name == 'name':
                table_item['importance'] = 'primary' 

            table_item['attributes'] += [attribute_item]

with open('mondial_relations.json', 'w') as f:
	json.dump(tables, f,indent=4)

with open('mondial_edges.json', 'w') as f:
	json.dump(relationships, f,indent=4)
