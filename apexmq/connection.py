import threading
import pika
from typing import Dict
from django.conf import settings
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

    def __init__(self, channel: BlockingChannel, queue_name):
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


class ApexMQChannelManager:
    """
    Manages the connection to RabbitMQ.

    Attributes:
        connection_name (str): The name of the connection configuration.
        connection_params (dict): Parameters used to establish the connection.
        connection (pika.BlockingConnection): The connection to RabbitMQ.
        channel_list (Dict[str, ApexMQChannelManager]): A dictionary to keep track of all channels in this connection.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to keep track of all queues across channels.
    """

    def __init__(self, connection_name):
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

    def __init__(self, connection_name):
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
        return channel_manager

    def close_connection(self):
        """
        Closes the RabbitMQ connection.
        """
        if self.connection:
            self.connection.close()
            print("RabbitMQ connection closed")

    def create_all_channels_and_queues(self):
        """
        Create all channels and queues specified in the connection configuration.

        This method reads the connection configuration and creates all specified
        channels and their associated queues. If no channels are specified, it
        creates a default channel with a default queue.

        The method populates the channel_list and queue_list attributes of the
        connection manager.

        Returns:
            None

        Raises:
            ImproperlyConfigured: If CHANNELS is declared but empty in the connection configuration.
        """
        if "CHANNELS" not in self.connection_params:
            new_channel = self.create_channel("default")
            DEFAULT_QUEUE_NAME = str(settings.ROOT_URLCONF).split(".")[0]
            new_queue = new_channel.create_queue(DEFAULT_QUEUE_NAME)
            self.queue_list[f"{new_channel.channel_name}-{DEFAULT_QUEUE_NAME}"] = (
                new_queue
            )
        else:
            channels_list = self.connection_params["CHANNELS"]
            if len(channels_list) == 0:
                raise ImproperlyConfigured(
                    f"If you declare CHANNELS in your '{self.connection_name}' connection you have to declare channels and QUEUES configurations. At least channels list and queue names in that channel."
                )
            else:
                for channel_name, channel_data in dict(channels_list).items():
                    new_channel = self.create_channel(channel_name)
                    new_queue_list = new_channel.create_all_queues(channel_data)
                    self.queue_list.update(new_queue_list)
