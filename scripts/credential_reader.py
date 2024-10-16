from typing import Optional
import configparser as cp
import os
import json


def get_terra_config_file_location() -> os.PathLike:
    '''Reads an environment variable and returns location of config files'''
    terra_config_file_location = os.environ.get('TERRA_CONFIG_LOC')
    if terra_config_file_location is None:
        raise Exception('got None when retreiving TERRA_CONFIG_LOC environment variable')
    return terra_config_file_location


def validate_config_fpath(config_fpath: os.PathLike) -> None:
    '''validates that any path-to-file exists,
        meant to ensure config file exists'''
    if not os.path.exists(config_fpath):
        raise Exception(f'config file at {config_fpath} does not exist')


def get_config_file_path(config_file_name: str) -> os.PathLike:
    '''Returns full path to the config file'''
    terra_config_file_location = get_terra_config_file_location()
    config_fpath = os.path.join(terra_config_file_location, config_file_name)
    validate_config_fpath(config_fpath)
    return config_fpath


def read_db_secrets(config_fpath: Optional[str] = None):
    """
    Returns a dictionary of database credentials with keys:
    'database' for the name of the postgres database
    'user' for the pg username
    'password' for the pg user password
    'host' for database host

    The credential file is assumed to be in at a location which
    is defined in the TERRA_CONFIG_LOC environment variable

    """

    if config_fpath is None:
        config_file_name = '.db.secrets.txt'
        config_fpath = get_config_file_path(config_file_name)

    config = cp.ConfigParser()
    config.read(config_fpath)
    db_creds = config['neurobooth.terra.db']
    credentials = {'database': db_creds['DB_Name'],
                   'user': db_creds['User'],
                   'password': db_creds['Password'],
                   'host': db_creds['Host']}
    return credentials


def read_dataflow_configs(config_fpath: Optional[str] = None):
    '''Returns a dictionary of parameters that is defined
        in the dataflow config json file. Will return a different
        datastructure of json structure changes.'''

    if config_fpath is None:
        config_file_name = 'dataflow_config.json'
        config_fpath = get_config_file_path(config_file_name)

    dataflow_configs = json.load(open(config_fpath))
    return dataflow_configs

