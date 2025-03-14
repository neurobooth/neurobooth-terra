from importlib import resources
import neurobooth_terra.views.sql as sql
from psycopg2._psycopg import connection


VIEWS = [  # Below list should be in intended order of view creation
    'rc_visit_dates_clean',
    'rc_ataxia_pd_scales_clean',
    'rc_clinical_clean',
    'rc_demographic_clean',
    'v_longitudinal_summary',                      # Depends on rc_clinical_clean, rc_demographic_clean
    'v_scale_bars',                                # Depends on rc_ataxia_pd_scales_clean
    'v_scale_sara',                                # Depends on rc_ataxia_pd_scales_clean
    'v_scale_micars',                              # Depends on rc_ataxia_pd_scales_clean
    'v_scale_updrs',                               # Depends on rc_ataxia_pd_scales_clean
    'v_scale_uhdrs',                               # Depends on rc_ataxia_pd_scales_clean
    'booth_query_krzysztof'
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
