from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "~/neurobooth-sandbox-358a72a54a08.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials)

# Bigquery REST API: https://cloud.google.com/bigquery/docs/reference/rest

query = """
    SELECT name, SUM(number) as total_people
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE state = 'TX'
    GROUP BY name, state
    ORDER BY total_people DESC
    LIMIT 20
"""
query_job = client.query(query)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))

# XXX: export to pandas?
