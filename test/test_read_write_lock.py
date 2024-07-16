import threading
import unittest
from time import sleep
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../threading_lib'))

from read_write_lock import FairReadWriteLock


class TestReadWriteLock(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = FairReadWriteLock()

    def test_read_lock(self):

        def reader():
            self.lock.acquire_read()
            print("read_lock acquired")
            sleep(0.1)
            self.lock.release_read()
            print("read_lock released")

        print("test read lock")
        read_thread1 = threading.Thread(target=reader)
        read_thread2 = threading.Thread(target=reader)

        read_thread1.start()
        read_thread2.start()

        read_thread1.join()
        read_thread2.join()

    def test_write_lock(self):
        def writer():
            self.lock.acquire_write()
            print("write_lock acquired")
            sleep(0.1)
            self.lock.release_write()
            print("write_lock released")


        print("test write lock")
        write_thread1 = threading.Thread(target=writer)
        write_thread2 = threading.Thread(target=writer)

        write_thread1.start()
        write_thread2.start()

        write_thread1.join()
        write_thread2.join()

    def test_read_write_lock(self):
        def reader():
            self.lock.acquire_read()
            print("read_lock acquired")
            sleep(0.1)
            self.lock.release_read()
            print("read_lock released")

        def writer():
            self.lock.acquire_write()
            print("write_lock acquired")
            sleep(0.1)
            self.lock.release_write()
            print("write_lock released")

            
        print("test read write lock")
        read_thread = threading.Thread(target=reader)
        write_thread = threading.Thread(target=writer)

        read_thread.start()
        sleep(0.05)  # Ensure read lock is acquired first
        write_thread.start()

        read_thread.join()
        write_thread.join()

if __name__ == '__main__':
    unittest.main()

  

    