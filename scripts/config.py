"""The SSH arguments and connection arguments."""
import os
from redcap import Project
import credential_reader as reader

secrets = reader.read_db_secrets()

db_args = secrets['db_args']
ssh_args = secrets['ssh_args']

dataflow_configs = reader.read_dataflow_configs()

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')
NB_WEAR_API_KEY = os.environ.get('WEARABLES_REDCAP_TOKEN')
FA_API_KEY = os.environ.get('FA_REDCAP_PROJECT')

if API_KEY is None:
    raise ValueError('Please define the environment variable NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)
wearables_project = Project(URL, NB_WEAR_API_KEY, lazy=True)
fa_project = Project(URL, FA_API_KEY, lazy=True)
