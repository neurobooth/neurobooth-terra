import os
import threading
import queue
import time

from numpy.testing import assert_allclose
import pandas as pd

import psycopg2
from neurobooth_terra import create_table
from neurobooth_terra.postgres import drop_table
from neurobooth_terra.redcap import (iter_interval, extract_field_annotation,
                                     map_dtypes, rename_subject_ids)

user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
connect_str = (f"dbname='neurobooth' user={user} host='localhost' "
               f"password={password}")


def _keyboard_interrupt(signal):
    while True:
        print('here')
        time.sleep(0.5)
        raise KeyboardInterrupt


def test_iter_interval():
    """Test iter_interval."""
    times = list()
    wait = 2.
    process_time = 0.2
    # check if iter_interval waits indeed for wait time
    for _ in iter_interval(wait=wait, exit_after=3.):
        times.append(time.time())
        time.sleep(process_time)
    assert_allclose(times[1] - times[0], wait + process_time, rtol=1e-1)

    """
    # check if keyboard interrupt works    
    signal = queue.Queue()
    thread = threading.Thread(target=_keyboard_interrupt,
                                args=(signal,))
    thread.daemon = True
    thread.start()
    for _ in iter_interval(wait=wait):
        time.sleep(process_time)
    """


def test_extract_field_annotation():
    """Test extraction of field annotation."""
    metadata = {'field_annotation': ['@blah', '@HIDDEN DB-y', 'DB-y FOI-gait',
                                     'FOI-gait-motor']}
    metadata_df = pd.DataFrame(metadata)
    metadata_df = metadata_df.apply(extract_field_annotation, axis=1)

    assert 'error' in metadata_df.columns
    assert 'gait' in metadata_df['FOI'].iloc[2]
    assert 'field_annotation reads:' in metadata_df['error'].iloc[3]


def test_map_dtypes():
    """Test mapping of dtypes."""
    metadata = {'field_type': ['calc', 'text'],
                'text_validation_type_or_show_slider_number':
                ['', 'date_mdy']
                }
    metadata_df = pd.DataFrame(metadata)
    metadata_df = metadata_df.apply(map_dtypes, axis=1)
    assert all(metadata_df['python_dtype'] == ['float64', 'str'])
    assert all(metadata_df['database_dtype'] == ['double precision', 'date'])


def test_rename_subject_ids():
    """Test renaming of subject."""
    table_id = 'subject'
    column_names = ['subject_id', 'redcap_event_name',
                    'first_name_birth', 'last_name_birth',
                    'date_of_birth_subject', 'old_subject_id']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)',
              'VARCHAR (255)', 'date', 'VARCHAR (255)']
    rows = [('1001', 'arm1', 'anoopum', 'gupta', '1985-11-28', None),
            ('1002', 'arm1', 'adonay', 'nunes', '1987-09-13', None)]
    index = {'subject_identifier': ['first_name_birth', 'last_name_birth',
                                    'date_of_birth_subject']}

    with psycopg2.connect(connect_str) as conn:
        drop_table(table_id, conn)
        table_subject = create_table(table_id, conn=conn,
                                     column_names=column_names,
                                     dtypes=dtypes, index=index)
        table_subject.insert_rows(rows, cols=column_names)

        # Simulate changing subject_id in redcap and updating old_subject_id.
        redcap_df = table_subject.query().reset_index()
        row_idx = redcap_df['subject_id'] == '1001'
        redcap_df.loc[row_idx, 'subject_id'] = '901'
        redcap_df.loc[row_idx, 'old_subject_id'] = '1001'
        # Add subject in redcap
        redcap_df = redcap_df.append(pd.DataFrame({
            'subject_id': ['1003'], 'first_name_birth': ['sheraz'],
            'last_name_birth': ['khan'], 'date_of_birth_subject': ['1980-05-15'],
            'old_subject_id': [None]
         }),
                                     ignore_index=True)

        rename_subject_ids(table_subject, redcap_df)

        # test renaming
        table_df_updated = table_subject.query().reset_index()
        assert '1001' in table_df_updated['old_subject_id'].values
        assert '1001' not in table_df_updated['subject_id'].values
        assert '901' in table_df_updated['subject_id'].values
        # only rename, don't add rows
        assert '1003' not in table_df_updated['subject_id'].values
