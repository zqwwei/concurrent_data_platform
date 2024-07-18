from abc import ABC, abstractmethod
import queue

class QueueInterface(ABC):
    @abstractmethod
    def put(self, item):
        pass

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def task_done(self):
        pass


class LocalQueue(QueueInterface):
    def __init__(self):
        self.q = queue.Queue()

    def put(self, item):
        self.q.put(item)

    def get(self):
        return self.q.get()
    
    def task_done(self):
        self.q.task_done()
 


