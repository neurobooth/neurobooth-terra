"""The SSH arguments and connection arguments."""
import os
from redcap import Project

ssh_args = dict(
        ssh_address_or_host='neurodoor.nmr.mgh.harvard.edu',
        ssh_username='mj513',
        ssh_config_file='~/.ssh/config',
        ssh_pkey='~/.ssh/id_rsa',
        remote_bind_address=('192.168.100.1', 5432),
        local_bind_address=('localhost', 6543)
)

db_args = dict(
    database='neurobooth', user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD']
)

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')

if API_KEY is None:
    raise ValueError('Please define the environment variable '
                     'NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)
