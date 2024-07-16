import threading

class FairReadWriteLock:
    """
    A lock object that allows many simultaneous "read locks", but only one "write lock".

    This is a variant of a read-write lock that supports multiple readers
    but only one writer at a time. It prevents write starvation by allowing
    writers to acquire the lock even if there are waiting readers.
    """
    def __init__(self):
        """Initialize a new ReadWriteLock."""
        self._writers_waiting = 0
        # number of active reader
        self._readers = 0
        # writer status
        self._writer = False
        self._lock = threading.Lock()
        # condition variable 
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

    def read_lock(self):
        """Return a context manager for a read lock"""
        return self._ReadLock(self)
    
    def write_lock(self):
        """Return a context manager for a write lock"""
        return self._WriteLock(self)

    class _ReadLock:
        """read lock context manager class"""

        def __init__(self, rw_lock):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_read()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rw_lock.release_read()


    class _WriteLock:
        """write lock context manager class"""

        def __init__(self, rw_lock):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_write()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.rw_lock.release_write()


