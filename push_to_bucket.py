from google.cloud import storage
from google.oauth2 import service_account

bucket_name = 'neurobooth'
project = 'neurobooth-sandbox'
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
scopes = ["https://www.googleapis.com/auth/cloud-platform"]

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

storage_client = storage.Client(credentials=credentials, project=project)

bucket = storage_client.bucket(bucket_name)
bucket.storage_class = "STANDARD"
new_bucket = storage_client.create_bucket(bucket, location="us")

print(
    "Created bucket {} in {} with storage class {}".format(
        new_bucket.name, new_bucket.location, new_bucket.storage_class
    )
)
