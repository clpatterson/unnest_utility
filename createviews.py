#!/usr/bin/env python
"""
GOAL: This module takes a nested BigQuery table and creates an individual 
table individual tables for the different levels of nesting. Specifically, 
it accesses a designated BQ table via user-provided table info and 
GCP project credentials. Then it parses table schema, writes a SQL query
to flatten the table, and executes a DDL statement to create a view for the 
flattened table.

Dear Recurse Reader: This program is a work in process. There are still a number
of edge cases it doesn't account for. 


"""

import os,re,json
from string import ascii_lowercase
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


# TODO: return file path of 'service_account.json'file on user system.
# HINT: use os.walk to search from User level down or Desktop down. 

# Set credentials
# NOTE: must be changed by user to reflect correct file path.
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/Users/charliepatterson/Documents/\
mhi_programs/bq-create-views-repo/service_account/service_account.json"

# Connect to a BigQuery project.
# NOTE: must be changed by user to reflect correct project.
PROJECT_NAME = 'cool-academy-217719'
client = bigquery.Client(project=PROJECT_NAME)

# Connect to a BigQuery dataset.
# NOTE: must be changed by user to reflect correct dataset.
DATASET_NAME = 'create_views_test'
dataset_ref = client.dataset(DATASET_NAME) # TODO: store dataset name in variable.

# Get schema for a table and pass list into a variable.
# NOTE: must be changed by user to reflect correct nested table.
table_name = 'ga_sessions_20170801'
table_ref = dataset_ref.table(table_name)
table = client.get_table(table_ref)
table_schema = table.schema
table_name = table_name.encode()

# Define primary key.
# NOTE: must be changed by user to reflect correct primary key.
primary_key = 'visitId'
primary_key = primary_key.encode()


def schema_to_dict(table_schema):
	"""Convert BigQuery table schema object to a list of nested Python dictionaries and lists."""
	fields = []
	for SchemaField in table_schema:
		field_dict = SchemaField.to_api_repr()
		fields.append(field_dict)
	return fields

fields = schema_to_dict(table_schema)
#print(json.dumps(fields, sort_keys=True, indent=4))

def trim_table_name(table_name):
	table_list = table_name.split(".")
	if len(table_list) == 1:
		return '.'.join(table_list)
	else:
		table_list.pop()
		return '.'.join(table_list)

def parse_table_schema(fields, table=table_name, new_fields=[]):
	"""Add a table name that preserves parentage for each field."""
	for field in fields:
		if field['name'] == primary_key: # Don't add table name to primary_key
			pass
		if field['mode'] != u'REPEATED' and field['type'] == u'RECORD': # Handle nullable records (structs)
			field['table_name'] = table + u'.' + field['name'] + u'(nullable_record)'
			#print(field['table_name'] + u'.' + field['name'])
			fields = field['fields']
			new_fields += parse_table_schema(fields, field['table_name'], [])
			#print('Recursion Over' + u' at ' + field['table_name'])
			table = trim_table_name(field['table_name'])
			#print(table)
		elif field['mode'] != u'REPEATED': # Handle nullable fields
			field['table_name'] = table
			#print(field['table_name'] + field['name'])
			new_fields.append(field)
		else:
			if field['type'] != u'RECORD' and field['mode'] == u'REPEATED': # Handle repeated fields (arrays)
				#field['table_name'] = table + u'.' + field['name']
				field['table_name'] = table
				#print(field['table_name'] + u'.' + field['name'])
				new_fields.append(field)
			else: # Handle repeated records (structs)
				field['table_name'] = table + u'.' + field['name']
				#print(field['table_name'] + u'.' + field['name'])
				fields = field['fields']
				new_fields += parse_table_schema(fields, field['table_name'], [])
				#print('Recursion Over' + u' at ' + field['table_name'])
				table = trim_table_name(field['table_name'])
				#print(table)
	return new_fields

fieldss = parse_table_schema(fields)

# What do I do with arrays?
# for now just use array_length(name) as name_count

#print(json.dumps(fieldss, sort_keys=True, indent=4))

# TODO:
def sort_fields(fields, table_dict={}):
	"""Sort fields into queries that will become unique views."""
	for field in fields:
		if field['table_name'] not in table_dict.keys():
			table_dict.update({field['table_name']:{'fields':[]}})
			table_dict[field['table_name']]['fields'].append(field)
		else:
			table_dict[field['table_name']]['fields'].append(field)
	return table_dict

f = sort_fields(fieldss)
#print(json.dumps(f, sort_keys=True, indent=4))
#test = f.keys()
#print(test)
#test2 = f.items()
#ttable = test2[6]
#print(ttable)
#tfields = ttable[1]['fields']
#tf1 = tfields[0]
#print(tf1['name'])

def sql_from(table):
	"""Write a sql from statement from a provided table name."""
	table_name = table[0]
	tables = table_name.split(".")
	# Nullable records do not need to be unnested in BigQuery
	tables = [t for t in tables if bool(re.search('nullable_record',t)) is False]
	from_clause = "from `{}.{}.{}` as a".format(PROJECT_NAME,DATASET_NAME,tables[0])
	n = 1
	aliases = []
	# Handle the unnest clause of the from statement
	for table in tables[1:]:
		alias = ascii_lowercase[n]
		aliases.append(alias)
		if len(aliases) == 1:
			unnest = ", unnest({}) as {}".format(table,alias)
		else:
			# Reference the unnested table that came before
			reference = aliases[-2]
			unnest = ", unnest({}.{}) as {}".format(reference,table,alias)
		from_clause += unnest
		n += 1
	return from_clause

#print(sql_from(test))

def  sql_select(table):
	"""Write a sql select statement from a list of fields."""
	fields = table[1]['fields']
	select_clause = "select {}".format(primary_key)
	tables = table[0].split(".")
	#print(tables)
	nullable_record_flag = bool(re.search('nullable_record',table[0]))
	# Find table alias (exclude nullable records because they aren't unnested in from clause)
	tables_count = len([t for t in tables if bool(re.search('nullable_record',t)) is False])
	table_alias = ascii_lowercase[tables_count -1]
	# Build full table path to be used when selecting fields
	table_reference_path = [t for t in tables if bool(re.search('nullable_record',t)) is True]
	#print(table_reference_path)
	table_reference_path = [re.sub('\(nullable_record\)','',t) for t in table_reference_path]
	#print(table_reference_path)
	table_reference_path = '.'.join(table_reference_path)
	#print(table_reference_path)
	# This is where my problem is...I'm only referring to the nullabe record in my 
	#nullable_record_name = re.sub('\(nullable_record\)','',tables[-1])
	#print(nullable_record_name)
	#print('nullable_record_name:'+ nullable_record_name)
	# Fields in nullable records must reference the record name
	if nullable_record_flag is True:
		table_alias = table_alias +"."+ table_reference_path
		#print(table_alias)
	if table_alias is 'a' and nullable_record_flag is False:
		pass
	else:	
		table_name = re.sub('\(nullable_record\)','',table[0])
		#print(table_name)
		field_alias_prefix = table_name.split(".",1)
		field_alias_prefix = field_alias_prefix[1].replace(".","_")
	for f in fields:
		if f['name'] == primary_key:
			pass
		else:
			field = table_alias + "." + f['name']
			if table_alias is 'a' and nullable_record_flag is False:
				field_alias = f['name']
			else:
				field_alias = field_alias_prefix + "_" + f['name']
			if f['mode'] == 'REPEATED':
				# Count array values instead of creating seperate tables for now
				field_clause = ", array_length({}) as {}".format(field,field_alias+'_count')
			else:
				field_clause = ", {} as {}".format(field,field_alias)
			select_clause += field_clause
	return select_clause

#print(sql_from(ttable))
#print(sql_select(ttable) + " " + sql_from(ttable))

# This function tables 
def sql_query(table_dict):
	"""Write sql queries for all my nested tables."""
	queries = {}
	for table in table_dict.items():
		sql_query = sql_select(table) + " " + sql_from(table)
		queries.update({table[0]:sql_query})
	return queries

tq = sql_query(f)
#print(tq)
#for q in tq.items():
#	print(q[0])
#	print(q[1])

def create_views(queries):
	"""Create views for queries in the provided project and dataset."""
	for query in queries.items():
		#print(query[0])
		print(query[1])
		table_name = re.sub('\(nullable_record\)','',query[0])
		#view_name = "vw_" + query[0].replace('.',"_")
		view_name = "vw_" + table_name.replace('.',"_")
		view_ref = dataset_ref.table(view_name)
		view = bigquery.Table(view_ref)
		view.view_query = query[1]
		view = client.create_table(view)
		print("Successfully created view at {}".format(view.full_table_id))
		#view_query = "create or replace {} as {};".format(view_name,query[1])
		#client.query(view_query)
	return
create_views(tq)
