import re
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset that contains
#                  the tables you are listing.
dataset_id = 'recurse-dev.create_views_test'

tables = client.list_tables(dataset_id)  # Make an API request.

# Delete all views prefixed with 'vw_' in the dataset
for table in tables:
    if bool(re.search('vw_',table.table_id)) is True:
        full_table_id = "{}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        client.delete_table(full_table_id, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(full_table_id))