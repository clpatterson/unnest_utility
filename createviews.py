#!/usr/bin/env python
"""
GOAL: This module creates views from nested tables in GCP BigQuery.
It accesses a designated BQ table via user-provided table info and 
GCP project credentials, parses table schema, writes a SQL query
to flatten the table, executes the query in BQ, then saves the view.

"""

import os,re
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


# TODO: return file path of 'service_account.json'file on user system.
# HINT: use os.walk to search from User level down or Desktop down. 

# Set credentials
# NOTE: must be changed by user to reflect correct file path.
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/Users/Work/Desktop/\
scripting/gcp/bq/service_account/service_account.json"

# Connect to a project.
project_name = 'cool-academy-217719'
client = bigquery.Client(project=project_name)

# Connect to a dataset.
dataset_name = 'create_views_test'
dataset_ref = client.dataset(dataset_name) # TODO: store dataset name in variable.

# Get schema for a table and pass list into a variable.
table_name = 'products_copy'
table_ref = dataset_ref.table(table_name)
table = client.get_table(table_ref)
table_schema = table.schema

# Define primary key
primary_key = '_id'

def unpack_table_schema(table_schema):
	"""Convert table schema to list of python dicts."""
	fields = []
	for SchemaField in table_schema:
		field_dict = SchemaField.to_api_repr()
		fields.append(field_dict)
	return fields

fields = unpack_table_schema(table_schema)

# Algorithm parses table schema.
def parse_table_schema(fields, table='', views=[]):
	"""Parse schema, track lineage, and return list of fields."""
	
	for field in fields:
		field_type = field['type']
		field_mode = field['mode']
		field_name_unicode = field['name']
		field_name = field_name_unicode.encode('ascii')
		# Take out primary key
		if field_name is primary_key:
			pass
		# Add record fields.
		if field_mode != u'REPEATED' and field_type == u'RECORD':
			if not table == '':
				table = table + '.' + field_name
				fields = field['fields']
				# Add records flag
				for field in fields:
					field['record'] = table
				views += parse_table_schema(fields, table, [])
				table = ''
			else:
				table = field_name
				fields = field['fields']
				# Add records flag
				for field in fields:
					field['record'] = table
				views += parse_table_schema(fields, table, [])
				table = ''
		# Add non-repeated fields.
		elif field_mode != u'REPEATED':
			view_name = table
			field_name = table + '.' + field_name
			# Check to if record key exits
			try:
				record = field['record']
			except KeyError:
				views.append({
					'view_names':view_name, 
					'field_name':field_name
					})
			else:
				# prevent record from appearing as view_name
				if record in table:
					view_name = re.sub(record, '', table)
				views.append({
					'view_names':view_name, 
					'field_name':field_name, 
					'record': record
					})
		else:
			# Add repeated non-record fields 
			if field['type'] != u'RECORD':
				view_name = table
				field_name = table + '.' + field_name
				method = 'ARRAY_LENGTH({})'.format(field_name)
				# Check to if record key exits
				try:
					record = field['record']
				except KeyError:
					views.append({
						'view_names':view_name, 
						'field_name':field_name, 
						'method':method
						})
				else:
					# prevent record from appearing as view_name
					if record in table:
						view_name = re.sub(record, '', table)
					views.append({'view_names':view_name, 
						'field_name':field_name, 
						'method':method, 
						'record': record})
			# Unpack repeated record fields
			else:
				if not table == '':
					table = table + '.' + field_name
					fields = field['fields']
					views += parse_table_schema(fields, table, [])
					table = ''
				else:
					table = field_name
					fields = field['fields']
					views += parse_table_schema(fields, table, [])
					table = ''

	return views

fields = parse_table_schema(fields)
for field in fields:
	print(field)


## TODO: Determine which views to create.
#def views_planning(fields):
#	view_name = []
#	views = []
#	for field in fields:
#		if not '.' in field:
#			view_name.append(table_name)
#			views.append({'view':table_name,'field':field})
#
#	return views
#
#print(views_planning(fields))


#def write_select_clause(fields):
#	"""Format fields for select clause of SQL query."""
#	select_string = ''
#	for field in fields:
#		if '_RECORD' in field:
#			field = field[:-7]
#		if 'R_' in field:
#			field = field.split('R')
#			field = field[0]
#		if '.' in field:
#			alias = re.sub(r'\.', '_', field)
#			select_string = select_string + field + ' AS ' + alias + ', ' 
#		else:
#			select_string = select_string + field + ', '
#	select_string = select_string[:-2]
#	
#	return select_string
#
#select = write_select_clause(fields)
#
#
#def write_table_path(project_name, dataset_name, table_name):
#	bq_table_path = '{0}.{1}.{2}'.format(project_name,
#		dataset_name,
#		table_name)
#	return bq_table_path
#
#bq_table_path = write_table_path(project_name, dataset_name, table_name)
#
#def write_from_clause(bq_table_path, fields):
#	# Cycle through fields and find columns to unnest
#	unnest = []
#	for field in fields:
#		if '_RECORD' in field:
#			pass
#		elif '.' in field:
#			index = [pos for pos, char in enumerate(field) if char == '.']
#			for value in index:
#				need_unnest = field[:value]
#				need_unnest = 'UNNEST({})'.format(need_unnest)
#				unnest.append(need_unnest)
#	
#	unnest = sorted(set(unnest))
#	unnest_string = ''
#	for field in unnest:
#		unnest_string = unnest_string + field + ', '
#	unnest_string = unnest_string[:-2]
#	
#	return unnest_string
#
#unnest_clause = write_from_clause(bq_table_path, fields)
#
#
## TODO: Algorithm to write SQL query.
#def assemble_top_level_query(select, bq_table_path, unnest_clause):
#	"""Write SQL query from clauses."""
#	if len(unnest_clause) is not 0:
#		comma = ','
#	else:
#		comma = ''
#
#	query = 'SELECT {0} FROM `{1}`{2}{3};'.format(
#		select,
#		bq_table_path,
#		comma,
#		unnest_clause)
#	
#	return query
#
#query = assemble_top_level_query(select,bq_table_path,unnest_clause)
#print(query)

# TODO: Add commas to select statements (last item has no comma after)
# TODO: Add UNNEST() to unnest statements
# TODO: Add commas to unnest statements.
# TODO: Execute query.
# TODO: Save query as view.
# TODO: Close connection.