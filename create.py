"""Create Bigquery tables

In [1]: %run create.py create -s /Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/
        register/Neurobooth-ConsentExport_DATA_2021-05-05_1409.csv --table-id consent
In [2]: %run create.py delete --table-id consent
"""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import json
import os.path as op
from optparse import OptionParser

import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

scopes = ["https://www.googleapis.com/auth/cloud-platform"]
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
dataset_id = 'register'
project = 'neurobooth-sandbox'

data_dir = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
            'register/')
schema_fname = op.join(data_dir, 'schema.json')

### Parse user input

parser = OptionParser()
parser.add_option("-s", "--source",
                  metavar="SOURCE", help="source csv file")
parser.add_option("-t", "--table-id",
                  metavar="TABLEID", help="Bigquery table ID")
(options, args) = parser.parse_args()

operation = args[0]
csv_fname = options.source
table_id = options.table_id

#### Input validation

if table_id not in ('consent', 'contact'):
    raise ValueError('Table ID must be one of consent/contact')

if operation not in ('create', 'append', 'delete'):
    raise ValueError('Please supply one of create/delete/append as an argument')

#### Authentication

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

client = bigquery.Client(credentials=credentials, project=project)

#### Create dataset if it does not exist

dataset_id_full = f'{client.project}.{dataset_id}'
table_id_full = f"{project}.{dataset_id}.{table_id}"

datasets = list(client.list_datasets())
if len(datasets) == 0:
    dataset = bigquery.Dataset(dataset_id_full)
    dataset = client.create_dataset(dataset)
elif datasets[0].dataset_id == dataset_id:
    dataset = datasets[0]

#### sort schema_json in same order as df columns
def _get_schema(schema_fname, csv_fname, table_id):
    with open(schema_fname, 'r') as fp:
        schema_json = json.load(fp)[table_id]
    df = pd.read_csv(csv_fname)
    schema = list()
    for key in df.columns:
        val = schema_json[key]
        schema.append(
            bigquery.SchemaField(val['name'], val['type'], val['mode'])
        )
    return schema

####  Create/append/delete table in dataset

tables = list(client.list_tables(dataset_id_full))
if operation == 'create':
    assert len(tables) == 0
    schema = _get_schema(schema_fname, csv_fname, table_id)
    table = bigquery.Table(table_id_full, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')
elif operation == 'append':
    assert tables[0].table_id == table_id
    schema = _get_schema(schema_fname, csv_fname, table_id)
    df = pd.read_csv(csv_fname)
    df = df.where(~df.isna(), None)

    table = tables[0]
    # data = [tuple(this_df[1].tolist()) for this_df in df.iterrows()]
    # errors = client.insert_rows(table, data, schema)
    # df.to_gbq(table_id_full, table_schema=list(schema_json.values()),
    #           credentials=credentials, if_exists='replace')

    errors = client.insert_rows_from_dataframe(table, df, schema)
    if errors != [[]]:
        raise ValueError(errors)

elif operation == 'delete':
    errors = client.delete_table(table_id_full)
    print('deleted table')
