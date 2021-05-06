import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

scopes = ["https://www.googleapis.com/auth/cloud-platform"]
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
dataset_id = 'register'
project = 'neurobooth-sandbox'
csv_fname = ('/Users/mainak/Documents/github_repos/neurobooth-terra/'
             'register/consent.csv')
table_id = 'consent'
schema = [
    bigquery.SchemaField("subject_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("study_id", "INTEGER", mode="REQUIRED"),
]

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

client = bigquery.Client(credentials=credentials, project=project)

dataset_id_full = f'{client.project}.{dataset_id}'
table_id_full = f"{project}.{dataset_id}.{table_id}"

datasets = list(client.list_datasets())
if len(datasets) == 0:
    dataset = bigquery.Dataset(dataset_id_full)
    dataset = client.create_dataset(dataset)
elif datasets[0].dataset_id == dataset_id:
    dataset = datasets[0]

tables = list(client.list_tables(dataset_id_full))

if len(tables) == 0:
    table = bigquery.Table(table_id_full, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f'Created table {table.project}.{table.dataset_id}.{table.table_id}')
elif tables[0].table_id == table_id:
    df = pd.read_csv(csv_fname)
    table = tables[0]
    client.insert_rows_from_dataframe(table, df, schema)
