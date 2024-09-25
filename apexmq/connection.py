import json
import logging
import pika
import time
from typing import Dict
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from .conf import get_connection_params


logger = logging.getLogger(__name__)


class ApexMQQueueManager:
    """
    Manages a specific queue in RabbitMQ.

    Attributes:
        channel (BlockingChannel): The channel used to interact with RabbitMQ.
        queue_name (str): The name of the queue.
        queue (pika.Queue): The declared queue instance.
        _queue_list (Dict[str, "ApexMQQueueManager"]): A class-level dictionary to keep track of all queue instances.
    """

    _queue_list: Dict[str, "ApexMQQueueManager"] = {}

    def __init__(self, channel: BlockingChannel, queue_name: str):
        """
        Initializes the ApexMQQueueManager.

        Args:
            channel (BlockingChannel): The channel used to interact with RabbitMQ.
            queue_name (str): The name of the queue.
        """
        self.channel = channel
        self.queue_name = queue_name
        self.queue = channel.queue_declare(queue=queue_name)
        self._queue_list[queue_name] = self
        logger.info(f"Queue created: {queue_name}")
        print(f"Queue created: {queue_name}")

    @classmethod
    def get_queue(cls, queue_name: str):
        if queue_name not in cls._queue_list:
            raise ImproperlyConfigured(
                f"Invalid queue name. Your choices are {list(cls._queue_list.keys())}"
            )
        return cls._queue_list[queue_name]


class ApexMQChannelManager:
    """
    Manages a specific channel in RabbitMQ.

    Attributes:
        connection (pika.BlockingConnection): The connection to RabbitMQ.
        channel_name (str): The name of the channel.
        channel (BlockingChannel): The created channel instance.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to keep track of all queues in this channel.
    """

    _channels_list: Dict[str, "ApexMQChannelManager"] = {}

    def __init__(self, connection: pika.BlockingConnection, channel_name: str):
        """
        Initializes the ApexMQChannelManager.

        Args:
            connection (pika.BlockingConnection): The connection to RabbitMQ.
            channel_name (str): The name of the channel.
        """
        self.connection = connection
        self.channel_name = channel_name
        self.channel = connection.channel()
        self.queue_list: Dict[str, ApexMQQueueManager] = {}
        self._channels_list[channel_name] = self

    @classmethod
    def get_channel(cls, channel_name):
        if channel_name not in cls._channels_list:
            raise ImproperlyConfigured(
                f"Invalid queue name. Your choices are {list(cls._channels_list.keys())}"
            )
        return cls._channels_list[channel_name]

    @classmethod
    def get_first_channel_name(cls):
        return list(cls._channels_list.keys())[0]

    def create_queue(self, queue_name: str):
        """
        Creates a queue in the channel.

        Args:
            queue_name (str): The name of the queue to create.
        """
        if queue_name not in self.queue_list:
            queue_manager = ApexMQQueueManager(self.channel, queue_name)
            self.queue_list[queue_name] = queue_manager
            return queue_manager
        return self.queue_list[queue_name]

    def publish(self, action: str, body: dict, to: str):
        """
        Publishes a message to the specified queue.

        Args:
            action (str): The action type of the message.
            body (dict): The message body.
            to (str): The name of the queue to publish the message to.
        """
        properties = pika.BasicProperties(action)
        try:
            queue_manager = self.create_queue(to)
            queue_manager.channel.basic_publish(
                exchange="",
                routing_key=to,
                body=json.dumps(body),
                properties=properties,
            )
        except Exception as e:
            logger.error(f"Failed to publish message to {to}: {e}")


class ApexMQConnectionManager:
    """
    Manages the connection to RabbitMQ.

    Attributes:
        connection_name (str): The name of the connection configuration.
        connection_params (dict): Parameters used to establish the connection.
        connection (pika.BlockingConnection): The connection to RabbitMQ.
        channel_list (Dict[str, ApexMQChannelManager]): A dictionary to keep track of all channels in this connection.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to keep track of all queues across channels.
    """

    def __init__(self, connection_name: str):
        """
        Initializes the ApexMQConnectionManager.

        Args:
            connection_name (str): The name of the connection configuration.
        """
        self.connection_name = connection_name
        self.connection_params = get_connection_params(connection_name)
        self.connection: pika.BlockingConnection = None
        self.channel_list: Dict[str, ApexMQChannelManager] = {}
        self.queue_list: Dict[str, ApexMQQueueManager] = {}
        # User credentials
        self.__USER__: str = self.connection_params["USER"]
        self.__PASSWORD__: str = self.connection_params["PASSWORD"]
        # Set default values
        self.__PORT__: int = self.connection_params.get("PORT", 5672)
        self.__HOST__: str = self.connection_params.get("HOST", "localhost")
        self.__VIRTUAL_HOST__: str = self.connection_params.get("VIRTUAL_HOST", "/")
        self.__CONNECT_RETRY_COUNT__: int = self.connection_params.get(
            "CONNECT_RETRY_COUNT", 5
        )
        self.__CONNECT_RETRY_WAIT__: int = self.connection_params.get(
            "CONNECT_RETRY_WAIT", 5
        )

    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Returns:
            pika.BlockingConnection: The established connection instance.

        Raises:
            ConnectionError: If unable to connect to RabbitMQ.
        """
        credentialis = pika.PlainCredentials(
            username=self.__USER__,
            password=self.__PASSWORD__,
        )
        connected = False
        error_msg = None
        for _ in range(self.__CONNECT_RETRY_COUNT__):
            try:
                connection_params = pika.ConnectionParameters(
                    host=self.__HOST__,
                    port=self.__PORT__,
                    virtual_host=self.__VIRTUAL_HOST__,
                    credentials=credentialis,
                )
                self.connection = pika.BlockingConnection(connection_params)
                connected = True
                logger.info(f"Connected to RabbitMQ: {self.connection_name}")
                print(f"Connected to RabbitMQ: {self.connection_name}")
                break
            except AMQPConnectionError as e:
                error_msg = e
                logger.error(f"Failed to connect to messege queue server: {error_msg}")

            time.sleep(self.__CONNECT_RETRY_WAIT__)
        if not connected:
            raise ConnectionError(
                f"Failed to connect to messege queue server: {error_msg}"
            )
        return self.connection

    def create_channel(self, channel_name: str) -> ApexMQChannelManager:
        """
        Creates and returns a channel manager for the specified channel name.

        Args:
            channel_name (str): The name of the channel to create.

        Returns:
            ApexMQChannelManager: The created channel manager.

        Raises:
            Exception: If the connection is not established.
        """
        if not self.connection:
            raise Exception("Connection not established. Call create_connection first.")

        channel_manager = ApexMQChannelManager(self.connection, channel_name)

        logger.info(f"Channel {channel_name} created.")
        print(f"Channel {channel_name} created.")
        return channel_manager

    def close_connection(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection:
            self.connection.close()
            logger.info("RabbitMQ connection closed")
            print("RabbitMQ connection closed")
