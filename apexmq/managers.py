import time, json, threading, atexit
from typing import Dict
import pika
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker
from django.core.exceptions import ImproperlyConfigured

from .conf import Logger, get_connection_settings
from .consumers import get_consumers_from_apps, BaseConsumer


class ApexMQConnectionManager:
    """
    A class to manage the connection to RabbitMQ.

    Attributes:
        connection (BlockingConnection): The connection object to RabbitMQ.
        params (ApexMQSettingsConnection): The connection settings.
        credentials (pika.PlainCredentials): The credentials to authenticate with Rabbit

    Methods:
        connect: Establishes a connection to RabbitMQ.    
    """
    def __init__(self):
        self.connection:BlockingConnection|None = None
        self.params = get_connection_settings()
        self.credentials = pika.PlainCredentials(
            self.params.user,
            self.params.password,
        )

    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Raises:
            ImproperlyConfigured: If the connection could not be established after multiple retries.
        
        Notes:
            - The connection is established using the BlockingConnection class from the pika library.
            - The connection parameters are fetched from the APEXMQ settings.
            - The connection is retried multiple times in case of failure.
        """
        retries = self.params.retries
        WAIT_TIME = 3
        while retries > 0:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.params.host,
                        credentials=self.credentials,
                        heartbeat=0
                    )
                )
                self.connection = connection
                Logger.info("Successfully connected to RabbitMQ.")
                break
            except AMQPConnectionError as e:
                Logger.error(f"Failed to connect to RabbitMQ: {e}. Retrying in {WAIT_TIME} seconds...")
            except Exception as e:
                Logger.error(f"Unexpected error: {e}")
            retries -= 1
            time.sleep(WAIT_TIME)

        if retries == 0 and self.connection is None:
            raise ImproperlyConfigured("Could not establish a RabbitMQ connection after multiple retries.")


class ApexMQProducerManager:
    """
    A class to manage the RabbitMQ producer.

    Attributes:
        connection (ApexMQConnection): The connection to RabbitMQ.
        channel (BlockingChannel): The channel for the producer.

    Methods:
        connect: Establishes a connection to RabbitMQ.
        ready: Creates a channel for the producer.
        create_channel: Creates a channel for the producer.
        publish: Publishes a message to a specified queue.
    
    Notes:
        - The producer uses the default exchange and routing key to publish messages.
        - The message content type is set to the action type.
    """
    connection = ApexMQConnectionManager()
    channel:BlockingChannel|None = None

    @classmethod
    def connect(cls):
        """
        Establishes a connection to RabbitMQ. 

        Raises:
            ImproperlyConfigured: If the connection could not be established.
        """
        cls.connection.connect()

    @classmethod
    def ready(cls):
        """
        Creates a channel for the producer.

        Raises:
            ImproperlyConfigured: If the RabbitMQ connection is not established.
        """
        cls.create_channel()
        atexit.register(cls.close)

    @classmethod
    def create_channel(cls):
        """
        Creates a channel for the producer.

        Raises:
            ImproperlyConfigured: If the RabbitMQ connection is not established.
        """
        if cls.connection.connection is None:
            raise ImproperlyConfigured("RabbitMQ connection is not established.")   
        
        cls.channel = cls.connection.connection.channel()

    @classmethod
    def publish(cls, action: str, body: dict, to: str):    
        """
        Publishes a message to a specified queue.

        Args:
            action (str): The action type of the message.
            body (dict): The message body as a dictionary.
            to (str): The name of the queue to publish the message to.
        
        Raises:
            Exception: If the message could not be published.
        
        Notes:
            - The message is published using the `basic_publish` method of the channel.
            - The message content type is set to the action type.
        """  
        try:
            cls.channel.basic_publish(
                exchange="",
                routing_key=to,
                body=json.dumps(body),
                properties=pika.BasicProperties(content_type=action)
            )
            Logger.info(f'"PUBLISHED - QUEUE: {to} | ACTION: {action}"')
        except Exception as e:
            Logger.error(f"Failed to publish message to {to}: {e}")
    
    @classmethod
    def close_channel(cls):
        """
        Closes the channel.

        Notes:
            - The method logs the closing of the channel.
            - The method calls the `close` method of the channel to close the channel.
        """
        if cls.channel and cls.channel.is_open:
            try:
                cls.channel.close()
            except Exception as e:
                Logger.error(f"Error closing producer channel: {e}")

    @classmethod
    def close_connection(cls):
        """
        Closes the connection.

        Notes:
            - The method logs the closing of the connection.
            - The method calls the `close` method of the connection to close the connection.
        """
        if cls.connection.connection and cls.connection.connection.is_open:
            try:
                cls.connection.connection.close()
            except Exception as e:
                Logger.error(f"Error closing producer connection: {e}")

    @classmethod
    def close(cls):
        """
        Closes the channel and connection.

        Notes:
            - The method calls the `close_channel` and `close_connection` methods to close the channel and connection.
        """
        try:
            cls.close_channel()
            cls.close_connection()
            Logger.debug("Closed producer channel and connection.")
        except Exception as e:
            Logger.error(f"Error closing producer: {e}")
    

class ApexMQConsumerManager:
    """
    A class to manage the RabbitMQ consumer.

    Attributes:
        connection (ApexMQConnection): The connection to RabbitMQ.
        channel (BlockingChannel): The channel for the consumer.
        queue_params (Dict[str, ApexMQQueue]): The queue parameters.
        consumers (Dict[str, BaseConsumer]): The consumer classes.

    Methods:
        connect: Establishes a connection to RabbitMQ.
        ready: Creates a channel, declares queues, and starts consuming messages.
        create_channel: Creates a channel for the consumer.
        declare_queues: Declares the queues based on the queue parameters.
        callback: The callback function to process consumed messages.
        consume: Starts consuming messages from the queues.
        start_consuming: Starts the consuming process.
        stop_consuming: Stops the consuming process.
    
    Notes:
        - The consumer uses the `basic_consume` method to consume messages from the queues.
        - The callback function processes the consumed messages based on the action type.
        - The consumer uses the `consumers` dictionary to map action types to consumer classes.
    """
    connection = ApexMQConnectionManager()

    def __init__(self):
        """
        Initializes the RabbitMQ consumer manager.

        Attributes:
            channel (BlockingChannel): The channel for the consumer.
            queue_params (Dict[str, ApexMQQueue]): The queue parameters.
            consumers (Dict[str, BaseConsumer]): The consumer classes.

        Notes:
            - The queue parameters are fetched from the APEXMQ settings.
            - The consumer classes are fetched from the installed apps.
            - The `consumers` dictionary maps action types to consumer classes.
        """
        self.channel:BlockingChannel|None = None
        self.queue_params = get_connection_settings().queue
        self.consumers:Dict[str, BaseConsumer] = get_consumers_from_apps()
        atexit.register(self.close)
    
    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Raises:
            ImproperlyConfigured: If the connection could not be established.
        """
        self.connection.connect()

    def ready(self):
        """
        Creates a channel, declares queues, and starts consuming messages.

        Raises:
            ImproperlyConfigured: If the RabbitMQ connection is not established.

        Notes:
            - The method calls the `create_channel`, `declare_queues`, `consume`, and `start_consuming' methods.
            - The consumer starts consuming messages after the connection is established.
            - The method logs the start of the consuming process.
            - The method is called after the connection is established.
        """
        self.create_channel()
        self.declare_queues()
        self.consume()
        self.start_consuming()

    def create_channel(self):
        """
        Creates a channel for the consumer.

        Raises:
            ImproperlyConfigured: If the RabbitMQ connection is not established.
        """
        if self.connection.connection is None:
            raise ImproperlyConfigured("RabbitMQ connection is not established.")   
        self.channel = self.connection.connection.channel()

    def declare_queues(self):
        """
        Declares the queues based on the queue parameters.

        Notes:
            - The method iterates over the queue parameters and calls the `model_dump` method to get the queue data.
            - The method declares the queue using the `queue_declare` method of the channel.
            - The method logs the declaration of each queue.
        """
        for queue_name, queue_params in self.queue_params.items():
            data = queue_params.model_dump()
            self.channel.queue_declare(queue=queue_name, **data)
            Logger.debug(f"Queue declared: {queue_name}")

    def callback(self, channel, method, properties, body):
        """
        The callback function to process consumed messages.

        Args:
            channel (BlockingChannel): The channel object.
            method (pika.spec.Basic.Deliver): The method object.
            properties (pika.spec.BasicProperties): The properties object.
            body (bytes): The message body as bytes.

        Notes:
            - The method extracts the action type and queue name from the properties and method objects.
            - The method logs the consumption of the message.
            - The method looks up the action type in the `consumers` dictionary to find the consumer class.
            - The method calls the consumer class with the action type and message body.
            - If no handler is found for the action type, a warning message is printed.
        """
        action_type = str(properties.content_type)
        queue_name = method.routing_key

        Logger.info(f'"CONSUMED - QUEUE: {queue_name} | ACTION: {action_type}"')

        lookup_prefix = action_type.split(".")[0]

        if lookup_prefix in self.consumers:
            ConsumerClass = self.consumers[lookup_prefix]
            try:
                ConsumerClass(action_type, body)
            except Exception as e:
                Logger.error(f"Failed to process consumer action: {e}")
        else:
            Logger.warning(f"No handler found for the action type: {action_type}")

    def consume(self):
        """
        Starts consuming messages from the queues.

        Notes:
            - The method iterates over the queue parameters and calls the `basic_consume` method of the channel.
            - The method sets the `on_message_callback` to the `callback` method.
            - The method sets `auto_ack` to `True` to automatically acknowledge messages after consumption.
        """
        for queue_name in self.queue_params.keys():
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self.callback,
                auto_ack=True
            )
    
    def start_consuming(self):
        """
        Starts the consuming process.

        Notes:
            - The method logs the start of the consuming process.
            - The method calls the `start_consuming` method of the channel to begin consuming messages.
        """
        self.channel.start_consuming()
        Logger.info("Started consuming messages.")

    def stop_consuming(self):
        """
        Stops the consuming process.

        Notes:
            - The method logs the stop of the consuming process.
            - The method calls the `stop_consuming` method of the channel to stop consuming messages.
        """
        if self.channel.is_open:
            try:
                self.channel.stop_consuming()
                Logger.debug("Stopping consuming messages.")
            except ChannelClosedByBroker as e:
                Logger.error(f"Channel closed by broker: {e}")
            except Exception as e:
                Logger.error(f"Error stopping consuming: {e}")

    def close_channel(self):
        """
        Closes the channel.

        Notes:
            - The method logs the closing of the channel.
            - The method calls the `close` method of the channel to close the channel.
        """
        if self.channel and self.channel.is_open:
            try:
                self.channel.close()
                Logger.debug("Closing cosumer channel.")
            except Exception as e:
                Logger.error(f"Error closing cosumer channel: {e}")

    def close_connection(self):
        """
        Closes the connection.

        Notes:
            - The method logs the closing of the connection.
            - The method calls the `close` method of the connection to close the connection.
        """
        if self.connection.connection and self.connection.connection.is_open:
            try:
                self.connection.connection.close()
                Logger.debug("Closing cosumer connection.")
            except Exception as e:
                Logger.error(f"Error closing cosumer connection: {e}")

    def close(self):
        """
        Closes the channel and connection.

        Notes:
            - The method calls the `close_channel` and `close_connection` methods to close the channel and connection.
        """
        self.stop_consuming()
        self.close_channel()
        self.close_connection()


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
            # self.producer_thread.join(timeout=1)
            self.consumer_thread.join(timeout=1)
        except Exception as e:
            Logger.error(f"Error closing threads: {e}")

        Logger.debug("Closed producer and consumer managers.")