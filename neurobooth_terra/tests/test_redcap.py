import threading
import queue
import time

from numpy.testing import assert_allclose

from neurobooth_terra.redcap import iter_interval


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

