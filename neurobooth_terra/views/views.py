from importlib import resources
import neurobooth_terra.views.sql as sql
from psycopg2._psycopg import connection


VIEWS = [  # Below list should be in intended order of view creation
    'rc_ataxia_pd_scales_clean',
    'rc_clinical_clean',
    'v_longitudinal_summary',  # Relies on rc_clinical_clean
]


def create_views(conn: connection, verbose: bool = False):
    with conn.cursor() as cursor:
        for view_name in VIEWS:
            if verbose:
                print(f'Creating view: {view_name}')

            view_sql = resources.read_text(sql, f'{view_name}.sql')
            cursor.execute(view_sql)


def drop_views(conn: connection, verbose: bool = False):
    with conn.cursor() as cursor:
        for view_name in reversed(VIEWS):  # Drop in reverse order of creation
            if verbose:
                print(f'Dropping view: {view_name}')

            cursor.execute(f'DROP VIEW IF EXISTS {view_name};')
