import threading

class FairReadWriteLock:
    def __init__(self):
        self._readers = 0
        self._writers_waiting = 0
        self._writer = False
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)

    def acquire_read(self):
        pass

    def release_read(self):
        pass

    def acquire_write(self):
        pass

    def release_write(self):
        pass
