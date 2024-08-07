import configparser as cp
import os


def read_db_secrets(db_secrets_file='.db.secrets.txt'):

    """
    Returns a dictionary of database credentials with keys:
    'database' for the name of the postgres database
    'user' for the pg username
    'password' for the pg user password

    The credential file is assumed to be in this folder. If it is not, replace the default argument
    or override the default in the calling script

    """

    path = os.path.join(os.path.dirname(__file__), db_secrets_file)
    config = cp.ConfigParser()
    config.read(path)
    db_creds = config['neurobooth.terra.db']
    credentials = {'database': db_creds['DB_Name'],
                   'user': db_creds['User'],
                   'password': db_creds['Password'],
                   'host': db_creds['Host']}
    return credentials
