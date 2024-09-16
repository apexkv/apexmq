import pika
from typing import Dict
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from .conf import get_connection_params


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

    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Returns:
            pika.BlockingConnection: The established connection instance.

        Raises:
            ConnectionError: If unable to connect to RabbitMQ.
        """
        credentialis = pika.PlainCredentials(
            username=self.connection_params["USER"],
            password=self.connection_params["PASSWORD"],
        )
        try:
            connection_params = pika.ConnectionParameters(
                host=self.connection_params.get("HOST", "localhost"),
                port=self.connection_params.get("PORT", 5672),
                virtual_host=self.connection_params.get("VIRTUAL_HOST", "/"),
                credentials=credentialis,
            )
            self.connection = pika.BlockingConnection(connection_params)
        except AMQPConnectionError as e:
            raise ConnectionError(f"Failed to connect to messege queue server: {e}")

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

        print(f"Channel {channel_name} created.")
        return channel_manager

    def close_connection(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection:
            self.connection.close()
            print("RabbitMQ connection closed")
