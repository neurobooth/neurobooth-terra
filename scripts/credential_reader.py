from typing import Optional, List
import os
import yaml
from pydantic import BaseModel, AnyUrl, PositiveInt, DirectoryPath, Field
from pydantic.networks import IPvAnyAddress
# import json
# import configparser as cp


class databaseArgs(BaseModel):
    db_name: str
    db_user: str
    password: str
    host: str | IPvAnyAddress | AnyUrl
    # this allows for values such as localhost or 127.0.0.1
    # or 192.168.xxx.xxx or <server_name>.nmr.mgh.harvard.edu


class dataflowArgs(BaseModel):
    reserve_threshold_bytes: PositiveInt
    suitable_volumes: List[DirectoryPath]
    delete_threshold: float = Field(ge=0, le=1)


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


def load_yaml_file_into_dict(yaml_file_path):
    with open(yaml_file_path) as yaml_file:
        param_dict = yaml.load(yaml_file, yaml.FullLoader)
        return param_dict


def read_db_secrets(config_fpath: Optional[str] = None):
    """
    Returns a dictionary of database credentials with keys:
    'database' for the name of the postgres database
    'user' for the pg username
    'password' for the pg user password
    'host' for database host

    The credential file is assumed to be at a location which
    is defined in the TERRA_CONFIG_LOC environment variable

    """

    if config_fpath is None:
        config_file_name = '.db.secrets.yml'
        config_fpath = get_config_file_path(config_file_name)

    db_config_dict = load_yaml_file_into_dict(config_fpath)
    db_args = databaseArgs(**db_config_dict)
    # this validates config values

    credentials = {'database': db_args.db_name,
                   'user': db_args.db_user,
                   'password': db_args.password,
                   'host': db_args.host}
    return credentials


def read_dataflow_configs(config_fpath: Optional[str] = None):
    '''
    Returns a dictionary of dataflow parameters with keys:
    'reserve_threshold_bytes' 
    'suitable_volumes' 
    'delete_threshold'
    '''

    if config_fpath is None:
        config_file_name = 'dataflow_config.yml'
        config_fpath = get_config_file_path(config_file_name)

    dataflow_config_dict = load_yaml_file_into_dict(config_fpath)
    dataflow_args = dataflowArgs(**dataflow_config_dict)
    # this validates dataflow config values

    dataflow_configs = {'reserve_threshold_bytes': dataflow_args.reserve_threshold_bytes,
                        'suitable_volumes': dataflow_args.suitable_volumes,
                        'delete_threshold': dataflow_args.delete_threshold}
    
    # check that all volumes in suitable volumes are actually suitable
    for volume in dataflow_configs['suitable_volumes']:
        if not os.path.exists(volume):
            raise Exception(f'volume path {volume} does not exist')

    return dataflow_configs


if __name__ == '__main__':
    '''Run this script standalone to test config reading or config value validation
       Pass config file paths as command line arguments'''
    import sys
    
    db_args = read_db_secrets(config_fpath=sys.argv[1])
    dataflow_args = read_dataflow_configs(config_fpath=sys.argv[2])

    for ky in db_args.keys():
        print(ky, db_args[ky])
    
    for ky in dataflow_args.keys():
        print(ky, dataflow_args[ky])

