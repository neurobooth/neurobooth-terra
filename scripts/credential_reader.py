from typing import Optional
import configparser as cp
import os


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
        
        db_secrets_fname='.db.secrets.txt'
        
        terra_config_file_location = os.environ.get('TERRA_CONFIG_LOC')
        if terra_config_file_location is None:
            raise Exception('got None when retreiving TERRA_CONFIG_LOC environment variable')
        
        config_fpath = os.path.join(terra_config_file_location, db_secrets_fname)
        if not os.path.exists(config_fpath):
            raise Exception(f'config file at {config_fpath} does not exist')

    config = cp.ConfigParser()
    config.read(config_fpath)
    db_creds = config['neurobooth.terra.db']
    credentials = {'database': db_creds['DB_Name'],
                   'user': db_creds['User'],
                   'password': db_creds['Password'],
                   'host': db_creds['Host']}
    return credentials
