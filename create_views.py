#!/usr/bin/env python
"""
GOAL: This module takes a nested BigQuery table, unnests it, and
creates individual views for each table. The root table primary key
is a field in each table, so the relationship between tables is
preserved. Specifically, this program accesses a designated BQ table
via user-provided table info and GCP project credentials. Then it 
recursively parses table schema, writes a SQL query to flatten the 
table, and executes a DDL statement to create a view for the flattened 
table.

CONTEXT: Nested tables present a challenge for traditional data profiling
tools that only accept flat tables. Writing queries by hand to unnest
tables for profiling is time consuming and dull. This program attempts
to free the BigQuery data engineer from this pain.

"""

import os,re,json, argparse
from string import ascii_lowercase
from google.cloud import bigquery

# Define cli interface
parser = argparse.ArgumentParser(description="unnest nested bigquery table")
parser.add_argument("project", help="gcp project where table is stored")
parser.add_argument("dataset", help="bigquery dataset where table is stored")
parser.add_argument("table", help="bigquery table to be unnested")
parser.add_argument("primary_key", help="table primary key")
args = parser.parse_args()

# Make list of BigQuery reserved keywords globally available
with open('/home/recurse_dev/bq-create-views/unnest_utility/bigquery_reserved_keywords.txt') as f:
    bq_keywords = f.read().splitlines()

# Set credentials
# TODO: This environment variable is something the user should set, if they are not
# running the program in cloud shell.
# NOTE: must be changed by user to reflect correct file path. (Not needed when run in Cloud Shell.)
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/Users/charliepatterson/Documents/\
# mhi_programs/bq-create-views-repo/service_account/service_account.json"

# Connect to a BigQuery project.
PROJECT_NAME = args.project
client = bigquery.Client(project=PROJECT_NAME)

# Connect to a BigQuery dataset.
DATASET_NAME = args.dataset
dataset_ref = client.dataset(DATASET_NAME)

# Get schema for a table and pass list into a variable.
TABLE_NAME = args.table
table_ref = dataset_ref.table(TABLE_NAME)
table = client.get_table(table_ref)
table_schema = table.schema
table_name = TABLE_NAME.encode()

# Define primary key.
# NOTE: must be changed by user to reflect correct primary key.
primary_key = args.primary_key
primary_key = primary_key.encode()

def schema_to_dict(table_schema):
	"""Convert BigQuery table schema object to a list of nested Python dictionaries and lists."""
	fields = []
	for SchemaField in table_schema:
		field_dict = SchemaField.to_api_repr()
		fields.append(field_dict)
	return fields

# For Debugging pursposes
#fields = schema_to_dict(table_schema)
#print(json.dumps(fields, sort_keys=True, indent=4))

def trim_table_name(table_name):
	table_list = table_name.split(".")
	if len(table_list) == 1:
		return '.'.join(table_list)
	else:
		table_list.pop()
		return '.'.join(table_list)

def parse_table_schema(fields, table=table_name, new_fields=[]):
	"""Add a table name that preserves table parentage for each field."""
	for field in fields:
		if field['name'] == primary_key: # Don't add table name to primary_key
			pass
		if field['mode'] != u'REPEATED' and field['type'] == u'RECORD': # Handle nullable records (structs)
			field['table_name'] = table + u'.' + field['name'] + u'(nullable_record)' # flag for query writing 
			fields = field['fields']
			new_fields += parse_table_schema(fields, field['table_name'], [])
			table = trim_table_name(field['table_name'])
		elif field['mode'] != u'REPEATED': # Handle nullable fields
			field['table_name'] = table
			new_fields.append(field)
		else:
			if field['type'] != u'RECORD' and field['mode'] == u'REPEATED': # Handle repeated fields (arrays)
				field['table_name'] = table
				new_fields.append(field)
			else: # Handle repeated records (structs)
				field['table_name'] = table + u'.' + field['name']
				fields = field['fields']
				new_fields += parse_table_schema(fields, field['table_name'], [])
				table = trim_table_name(field['table_name'])
	return new_fields

# For Debugging pursposes
#fieldss = parse_table_schema(fields)
#print(json.dumps(fieldss, sort_keys=False, indent=4))

def sort_fields(fields, table_dict={}):
	"""Sort fields into their respective table dictionaries."""
	for field in fields:
		if field['table_name'] not in table_dict.keys():
			table_dict.update({field['table_name']:{'fields':[]}})
			table_dict[field['table_name']]['fields'].append(field)
		else:
			table_dict[field['table_name']]['fields'].append(field)
	return table_dict

# For Debugging pursposes
#f = sort_fields(fieldss)
#print(json.dumps(f, sort_keys=True, indent=4))
#test = f.keys()
#print(test)
# Testing to understand how to iterate over a dictionary
#  and access certain keys and values
#test2 = f.items()
#for item in test2:
#    print(item)
#ttable = test2[6]
#print(ttable)
#tfields = ttable[1]['fields']
#tf1 = tfields[0]
#print(tf1['name'])

def sql_from(table):
	"""Write a sql from statement to build a table."""
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

# For Debugging pursposes
#print(sql_from(test))


def handle_bq_keyword(field_name):
    """Wrap field names with backticks if they are BigQuery reserved keywords."""
    keyword_check = [field_name.lower() == keyword.lower() for keyword in bq_keywords]
    if any(keyword_check) is True:
        field_name = '`{}`'.format(field_name)
    else:
        pass
    return field_name

def  sql_select(table):
	"""Write a sql select statement to build a table."""
	fields = table[1]['fields']
    # insert reserved key word handling for primary key
	select_clause = "select {}".format(handle_bq_keyword(primary_key))
	tables = table[0].split(".")
	nullable_record_flag = bool(re.search('nullable_record',table[0]))
	# Find table alias (exclude nullable records because they aren't unnested in from clause)
	tables_count = len([t for t in tables if bool(re.search('nullable_record',t)) is False])
	table_alias = ascii_lowercase[tables_count -1]
	# Build full table path to be used when selecting fields
	table_reference_path = [t for t in tables if bool(re.search('nullable_record',t)) is True]
	table_reference_path = [re.sub('\(nullable_record\)','',t) for t in table_reference_path]
	table_reference_path = '.'.join(table_reference_path)
	# Fields in nullable records must use table_reference_path
	if nullable_record_flag is True:
		table_alias = table_alias +"."+ table_reference_path
	if table_alias is 'a' and nullable_record_flag is False:
		pass
	else:	
		table_name = re.sub('\(nullable_record\)','',table[0])
		field_alias_prefix = table_name.split(".",1)
        # Remove the top level parent table name b/c it does not provide descriptive value
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
				field_clause = ", array_length({}) as {}".format(handle_bq_keyword(field),field_alias+'_count')
			else:
				field_clause = ", {} as {}".format(handle_bq_keyword(field),field_alias)
			select_clause += field_clause
	return select_clause

# For Debugging pursposes
#print(sql_from(ttable))
#print(sql_select(ttable) + " " + sql_from(ttable))

def sql_query(table_dict):
	"""Prepare a sql query for all unnested tables."""
	queries = {}
	for table in table_dict.items():
		sql_query = sql_select(table) + " " + sql_from(table)
		queries.update({table[0]:sql_query})
	return queries

# For Debugging pursposes
#tq = sql_query(f)
#print(tq)
#for q in tq.items():
#	print(q[0])
#	print(q[1])

def create_views(queries):
	"""Create views for unnested tables in the provided BigQuery project and dataset."""
	for query in queries.items():
		#print(query[1])
		table_name = re.sub('\(nullable_record\)','',query[0])
		view_name = "vw_" + table_name.replace('.',"_")
		view_ref = dataset_ref.table(view_name)
		view = bigquery.Table(view_ref)
		view.view_query = query[1]
		view = client.create_table(view)
		print("Successfully created view at {}".format(view.full_table_id))
	return

# For Debugging pursposes
#create_views(tq)

# Run on give table
create_views(sql_query(sort_fields(parse_table_schema(schema_to_dict(table_schema)))))