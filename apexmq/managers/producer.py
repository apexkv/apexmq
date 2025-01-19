import json, atexit, pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from .connection import ApexMQConnectionManager
from apexmq.conf import Logger



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
        if not cls.connection:
            raise ImproperlyConfigured("Connection manager is not established.")
        
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
            AMQPConnectionError: If the RabbitMQ connection is not open.
        """
        if not cls.connection.connection:
            raise ImproperlyConfigured("RabbitMQ connection is not established.")   
        
        if not cls.connection.connection.is_open:
            raise AMQPConnectionError("RabbitMQ connection is not open.")
        
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
        if not cls.connection.connection:
            raise ImproperlyConfigured("RabbitMQ connection is not established.")
        
        if not cls.channel:
            raise ImproperlyConfigured("RabbitMQ channel is not established.")
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
                Logger.debug("Closed producer channel.")
            except Exception as e:
                Logger.error(f"Error closing producer channel: {e}")
                raise e
        else:
            Logger.debug("Producer channel is already closed.")
            raise Exception("Producer channel is already closed.")

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
                cls.connection.close()
                Logger.debug("Closed producer connection.")
            except Exception as e:
                Logger.error(f"Error closing producer connection: {e}")
                raise e
        else:
            Logger.debug("Producer connection is already closed.")
            raise Exception("Producer connection is already closed.")

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
  