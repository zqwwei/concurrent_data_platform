import threading
import unittest
from time import sleep
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../threading_lib'))

from read_write_lock import FairReadWriteLock


class TestReadWriteLock(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = FairReadWriteLock()

    def reader(self):
        with self.lock.read_lock():
            print("read_lock acquired")
            sleep(0.1)
            print("read_lock released")

    def writer(self):
        with self.lock.write_lock():
            print("write_lock acquired")
            sleep(0.1)
            print("write_lock released")

    def test_read_lock(self):
        print("test read lock")
        read_thread1 = threading.Thread(target=self.reader)
        read_thread2 = threading.Thread(target=self.reader)

        read_thread1.start()
        read_thread2.start()

        read_thread1.join()
        read_thread2.join()

    def test_write_lock(self):
        print("test write lock")
        write_thread1 = threading.Thread(target=self.writer)
        write_thread2 = threading.Thread(target=self.writer)

        write_thread1.start()
        write_thread2.start()

        write_thread1.join()
        write_thread2.join()

    def test_read_write_lock(self):            
        print("test read write lock")
        read_thread = threading.Thread(target=self.reader)
        write_thread = threading.Thread(target=self.writer)

        read_thread.start()
        sleep(0.05)  # Ensure read lock is acquired first
        write_thread.start()

        read_thread.join()
        write_thread.join()

if __name__ == '__main__':
    unittest.main()

  

    