#!/usr/bin/env python
"""
GOAL: This module creates views from nested tables in GCP BigQuery.
It accesses a designated BQ table via user-provided table info and 
GCP project credentials, parses table schema, writes a SQL query
to flatten the table, executes the query in BQ, then saves the view.

"""

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField


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

def unpack_table_schema(table_schema):
	"""Convert table schema to list of python dicts."""
	fields = []
	for SchemaField in table_schema:
		field_dict = SchemaField.to_api_repr()
		fields.append(field_dict)
	return fields

fields = unpack_table_schema(table_schema)

# TODO: Algorithm parses table schema.
def parse_table_schema(fields, table='', select_statement=[]):
	"""Parse table schema and return values."""
	
	# Loop through fields and identify repeated fields
	for field in fields:
		field_type = field['type']
		field_mode = field['mode']
		#print(field_mode)
		field_name_unicode = field['name']
		field_name = field_name_unicode.encode('ascii')
		#print(field_name)
		if field_mode != u'REPEATED':
			select_statement.append(table + '.' + field_name)
		elif field_mode != u'REPEATED' and field_type == u'RECORD':
			parent = field_name
			for field in field['fields']:
				field_name_unicode = field['name']
				field_name = field_name_unicode.encode('ascii')
				select_statement.append(parent + '.' + field_name)
		else:
			# How do I deal with parent field name?
			#print(table)
			if field['type'] != u'RECORD':
				select_statement.append('R' + table + '.' + field_name)
			else:
				table = table + '.' + field_name
				fields = field['fields']
				#print(fields)
				#print(select_statement)
				select_statement += parse_table_schema(fields, table, [])
				table = ''


	return select_statement


result = parse_table_schema(fields)
print(result)


			#v, j = parse_table_schema(field)
			





#			print('is stinky')
#			parent = field_name
#			print(parent)
#			unnest_statement.append(parent)
#			nested_fields = field_dict['fields']
#			type(nested_fields)
#			# Create select statements for each nested field.
#			for field in nested_fields:
#				child_unicode = field['name']
#				child = child_unicode.encode('ascii')
#				column_name = '{}.{}'.format(parent,child)
#				column_alias = '{}_{}'.format(parent,child)
#				select_statement_line = '{} AS {}'.format(column_name,column_alias)
#				print(select_statement_line)
#				# Append select statement
#				select_statement.append(select_statement_line)
#		# Append non-nested field select statement
#		else:
#			select_statement.append(field_name)
#
#	return select_statement, unnest_statement
#
#print(parse_table_schema(table_schema))	
			
## TODO: Algorithm to write SQL query.
#def write_query():
#	"""Write SQL query from table schema."""
#
#	# TODO: Add commas to select statements (last item has no comma after)
#	# TODO: Add UNNEST() to unnest statements
#	# TODO: Add commas to unnest statements.
#	#   First item has needs comma space at front. Last item has 
#	#   no comma after.
#
#	# Create basic SQL query template.
#	bq_table_path = '{0}.{1}.{2}'.format(project_name,
#		dataset_name,
#		table_name)
#	
#	query = 'SELECT {0} FROM `{1}`{2};'.format(
#		select_statement,
#		bq_table_path,
#		unnest_statement)
#
## TODO: Execute query.
## TODO: Save query as view.
## TODO: Close connection.


