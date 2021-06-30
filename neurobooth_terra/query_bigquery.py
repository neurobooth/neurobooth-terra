# Authors: Mainak Jas <mjas@mgh.harvard.edu>

from google.cloud import bigquery
from google.oauth2 import service_account

scopes = ["https://www.googleapis.com/auth/cloud-platform"]
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
project = 'neurobooth-sandbox'
dataset_id = 'register'
table_id = 'contact'

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

client = bigquery.Client(credentials=credentials, project=project)
table_id_full = f"{project}.{dataset_id}.{table_id}"

# Bigquery REST API: https://cloud.google.com/bigquery/docs/reference/rest

query = f"""
    SELECT contact_first_name, contact_last_name, contact_email, n_falls_6_mo
    FROM `neurobooth-sandbox.register.contact` as contact
    JOIN `neurobooth-sandbox.register.falls` as falls
    ON contact.record_id = falls.record_id
    WHERE falls.n_falls_6_mo >= 2
"""
query_job = client.query(query)  # Make an API request.

for idx, row in enumerate(query_job):
    if idx == 0:
        print(row.keys())
    print(row.values())
