import atexit
from typing import Dict
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from apexmq.conf import Logger, get_connection_settings
from apexmq.consumers import get_consumers_from_apps, BaseConsumer
from .connection import ApexMQConnectionManager


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
        self.queue_params = get_connection_settings().queues
        self.consumers:Dict[str, BaseConsumer] = get_consumers_from_apps()
        atexit.register(self.close)
    
    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Raises:
            ImproperlyConfigured: If the connection could not be established.
        """
        if not self.connection:
            raise ImproperlyConfigured("Connection manager is not established.")
        
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
        if not self.connection.connection or not self.connection.connection.is_open:
            raise AMQPConnectionError("RabbitMQ connection is not established.")   
        
        self.channel = self.connection.connection.channel()

    def declare_queues(self):
        """
        Declares the queues based on the queue parameters.

        Notes:
            - The method iterates over the queue parameters and calls the `model_dump` method to get the queue data.
            - The method declares the queue using the `queue_declare` method of the channel.
            - The method logs the declaration of each queue.
        """
        if self.channel is None:
            raise ImproperlyConfigured("RabbitMQ channel is not established.")

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
        if self.channel is None:
            raise ImproperlyConfigured("RabbitMQ channel is not established.")

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
        if self.channel is None:
            raise AMQPConnectionError("RabbitMQ channel is not established.")
        
        self.channel.start_consuming()
        Logger.info("Started consuming messages.")

    def stop_consuming(self):
        """
        Stops the consuming process.

        Notes:
            - The method logs the stop of the consuming process.
            - The method calls the `stop_consuming` method of the channel to stop consuming messages.
        """
        if not self.channel or not self.channel.is_open:
            Logger.error("RabbitMQ channel is not established.")
            raise AMQPConnectionError("RabbitMQ channel is not established.")
        
        try:
            self.channel.stop_consuming()
            Logger.debug("Stopping consuming messages.")
        except Exception as e:
            Logger.error(f"Error stopping consuming: {e}")

    def close_channel(self):
        """
        Closes the channel.

        Notes:
            - The method logs the closing of the channel.
            - The method calls the `close` method of the channel to close the channel.
        """
        if not self.channel or not self.channel.is_open:
            raise AMQPConnectionError("Consumer channel is already closed.")
        
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
        if not self.connection or not self.connection.connection.is_open:
            Logger.error("Consumer connection is already closed.")
            raise AMQPConnectionError("Consumer connection is already closed.")
        try:
            self.connection.close()
            Logger.debug("Closing cosumer connection.")
        except Exception as e:
            Logger.error(f"Error closing cosumer connection: {e}")

    def close(self):
        """
        Closes the channel and connection.

        Notes:
            - The method calls the `close_channel` and `close_connection` methods to close the channel and connection.
        """
        try:
            self.stop_consuming()
            self.close_channel()
            self.close_connection()
            Logger.debug("Closed consumer channel and connection.")
        except Exception as e:
            Logger.error(f"Error closing consumer: {e}")