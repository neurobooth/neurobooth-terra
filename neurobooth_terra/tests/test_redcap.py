import threading
import queue
import time

from numpy.testing import assert_allclose
import pandas as pd

from neurobooth_terra.redcap import (iter_interval, extract_field_annotation,
                                     map_dtypes)


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
