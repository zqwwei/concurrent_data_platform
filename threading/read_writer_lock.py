import threading

class FairReadWriteLock:
    def __init__(self):
        # number of active reader
        self._readers = 0
        self._writers_waiting = 0
        # writer status
        self._writer = False
        self._lock = threading.Lock()
        self._read_ready = threading.Condition(self._lock)

    def acquire_read(self):
        with self._read_ready:
            while self._writer or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self):
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self):
        with self._read_ready:
            self._writers_waiting += 1
            while self._writer or self._readers > 0:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer = True

    def release_write(self):
        with self._read_ready:
            self._writer = False
            self._read_ready.notify_all()
