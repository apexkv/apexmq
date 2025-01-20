import threading, time, atexit

from apexmq.conf import Logger
from .producer import ApexMQProducerManager
from .consumer import ApexMQConsumerManager


class ApexMQManager:   
    """
    A class to manage the RabbitMQ connections and channels.

    Attributes:
        producer (ApexMQProducerManager): The producer manager.
        consumer (ApexMQConsumerManager): The consumer manager.

    Methods:
        connect: Establishes connections to RabbitMQ for the producer and consumer.
        ready: Starts the producer and consumer managers.

    Notes:
        - The manager class initializes the producer and consumer managers.
        - The manager class starts the producer and consumer managers.
    """ 
    def __init__(self):
        self.producer = ApexMQProducerManager()
        self.consumer = ApexMQConsumerManager()
        atexit.register(self.close)

    def connect(self):
        """
        Establishes connections to RabbitMQ for the producer and consumer.

        Notes:
            - The method starts the producer and consumer threads.
            - The method logs the successful connection to RabbitMQ.
        """
        def connect_producer():
            while True:
                try:
                    self.producer.connect()
                    break
                except Exception as e:
                    Logger.error(f"Failed to connect to producer: {e}")
                time.sleep(3)
            self.producer.ready()
        
        def connect_consumer():
            while True:
                try:
                    self.consumer.connect()
                    break
                except Exception as e:
                    Logger.error(f"Failed to connect to consumer: {e}")
                time.sleep(3)
            self.consumer.ready()

        self.producer_thread = threading.Thread(target=connect_producer, name="ProducerThread", daemon=True)
        self.consumer_thread = threading.Thread(target=connect_consumer, name="ConsumerThread", daemon=True)

        self.producer_thread.start()
        self.consumer_thread.start()

    def ready(self):
        """
        Starts the producer and consumer managers.

        Notes:
            - The method establishes connections to RabbitMQ for the producer and consumer.
        """
        self.connect()

    def close(self):
        """
        Closes the producer and consumer managers.

        Notes:
            - The method calls the `close` method of the producer and consumer managers.
        """

        try:
            self.producer_thread.join(timeout=1)
            self.consumer_thread.join(timeout=1)
        except Exception as e:
            Logger.error(f"Error closing threads: {e}")

        Logger.debug("Closed producer and consumer managers.")