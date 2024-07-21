from abc import ABC, abstractmethod
import queue
import pika

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

    @abstractmethod
    def close(self):
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

    def close(self):
        self.q.put(None)
 

class RabbitMQQueue(QueueInterface):
    def __init__(self, queue_name='task_queue'):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
    
    def put(self, item):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=item,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def get(self, count=1):
        messages = []
        for _ in range(count):
            method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name)
            if method_frame:
                #  make sure messages processed
                self.channel.basic_ack(method_frame.delivery_tag)
                messages.append(body)
            else:
                break
        return messages
    
    def task_done(self):
        pass

    def close(self):
        self.connection.close()
