"""Push files to Google bucket."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import os
import os.path as op

from tqdm import tqdm

from google.cloud import storage
from google.oauth2 import service_account

bucket_name = 'neurobooth'
project = 'neurobooth-sandbox'
key_path = "/Users/mainak/neurobooth-sandbox-358a72a54a08.json"
scopes = ["https://www.googleapis.com/auth/cloud-platform"]

data_dir = '/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data'
fname = 'sheraz_mouse_task.avi'
chunk_size = 5 * 1024 ** 2 # 5 MB

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=scopes)

storage_client = storage.Client(credentials=credentials, project=project)
bucket_names = [bucket.name for bucket in storage_client.list_buckets()]

bucket = storage_client.bucket(bucket_name)
if bucket_name not in bucket_names:
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )

blob = bucket.blob(fname)
source_fname = op.join(data_dir, fname)
# https://github.com/googleapis/python-storage/issues/27#issuecomment-651468428
print(f'{fname}->{bucket_name}')
with open(source_fname, "rb") as in_file:
    total_bytes = os.fstat(in_file.fileno()).st_size
    with tqdm.wrapattr(in_file, "read", total=total_bytes,
                       miniters=1, desc=f'uploading') as file_obj:
        while in_file.tell() < total_bytes:
            size = min(chunk_size, total_bytes - in_file.tell())
            blob.upload_from_file(file_obj, size=size, checksum='md5')
