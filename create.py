import json
import os.path as op
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

scopes = ["https://www.googleapis.com/auth/cloud-platform"]
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
dataset_id = 'register'
project = 'neurobooth-sandbox'

data_dir = ('/Users/mainak/Documents/github_repos/neurobooth-terra/'
            'register/')
schema_fname = op.join(data_dir, 'schema.json')
csv_fname = op.join(data_dir,
                    'Neurobooth-ConsentExport_DATA_2021-05-05_1409.csv')
table_id = 'consent'

with open(schema_fname, 'r') as fp:
    schema_json = json.load(fp)[table_id]
schema =[bigquery.SchemaField(v['name'], v['type'], v['mode'])
         for k, v in schema_json.items()]

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

client = bigquery.Client(credentials=credentials, project=project)

dataset_id_full = f'{client.project}.{dataset_id}'
table_id_full = f"{project}.{dataset_id}.{table_id}"

# Create dataset
datasets = list(client.list_datasets())
if len(datasets) == 0:
    dataset = bigquery.Dataset(dataset_id_full)
    dataset = client.create_dataset(dataset)
elif datasets[0].dataset_id == dataset_id:
    dataset = datasets[0]

# Create table in dataset
tables = list(client.list_tables(dataset_id_full))

if len(tables) == 0:
    table = bigquery.Table(table_id_full, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')
elif tables[0].table_id == table_id:
    df = pd.read_csv(csv_fname)
    table = tables[0]
    client.insert_rows_from_dataframe(table, df, schema)
