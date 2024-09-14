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
    Manages individual queues within a channel.

    This class is responsible for declaring and keeping track of queues
    within a specific channel. It provides a way to interact with a single queue.

    Attributes:
        _queue_list (Dict[str, 'ApexMQQueueManager']): A class-level dictionary to store all queue instances.
        channel (BlockingChannel): The channel associated with this queue.
        queue_name (str): The name of the queue.
        queue: The queue object returned by pika's queue_declare method.
    """

    _queue_list: Dict[str, "ApexMQQueueManager"] = {}

    def __init__(self, channel: BlockingChannel, queue_name):
        self.channel = channel
        self.queue_name = queue_name
        self.queue = channel.queue_declare(queue=queue_name)
        self._queue_list[self.queue_name] = self

    def basic_consume(self, callback):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=True
        )


class ApexMQChannelManager:
    """
    Manages channels within a connection.

    This class is responsible for creating and managing channels within a
    RabbitMQ connection. It also manages queues within each channel.

    Attributes:
        _channel_list (Dict[str, 'ApexMQChannelManager']): A class-level dictionary to store all channel instances.
        connection (pika.BlockingConnection): The connection associated with this channel.
        channel_name (str): The name of the channel.
        channel: The channel object created from the connection.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to store queues associated with this channel.
    """

    _channel_list: Dict[str, "ApexMQChannelManager"] = {}

    def __init__(self, connection: pika.BlockingConnection, channel_name):
        threading.Thread.__init__(self)
        self.connection = connection
        self.channel_name = channel_name
        self.channel = connection.channel()
        self.queue_list: Dict[str, ApexMQQueueManager] = {}
        self._channel_list[self.channel_name] = self
        self._stop_event = threading.Event()

    def create_queue(self, queue_name):
        """
        Create a new queue with the given name and add it to the queue list.

        This method creates a new ApexMQQueueManager instance, which in turn
        declares a new queue on the RabbitMQ server.

        Args:
            queue_name (str): The name of the queue to create.

        Returns:
            ApexMQQueueManager: A new queue manager instance.
        """
        new_queue = ApexMQQueueManager(self.channel, queue_name)
        self.queue_list[queue_name] = new_queue
        return new_queue

    def create_all_queues(self, channel_data: dict):
        """
        Create all queues specified in the channel configuration.

        This method reads the queue configuration from the channel_data and creates
        all specified queues for the current channel.

        Args:
            channel_data (dict): A dictionary containing the channel configuration,
                                 including a 'QUEUES' key with queue definitions.

        Returns:
            Dict[str, ApexMQQueueManager]: A dictionary of created queue managers,
                                           with keys formatted as '{channel_name}-{queue_name}'.

        Raises:
            ImproperlyConfigured: If no queues are defined in the channel configuration.
        """
        if "QUEUES" not in channel_data:
            raise ImproperlyConfigured(
                f"You need to declare 1 or more queue in QUEUES section in channel '{self.channel_name}' because you have declared the channel."
            )

        queue_list = dict(channel_data["QUEUES"])
        new_queue_list = {}

        for queue_name, queue_params in queue_list.items():
            new_queue_list[f"{self.channel_name}-{queue_name}"] = self.create_queue(
                queue_name
            )

        return new_queue_list

    def stop(self):
        """
        Stop the channel thread by setting the stop event and closing the connection.
        """
        print(f"Stopping channel {self.channel_name}.")
        self._stop_event.set()

    def run(self):
        """
        Start the channel thread, consuming messages from its queues.
        This method is called when the thread starts and continuously
        consumes messages until the stop event is set.
        """
        print(f"Channel {self.channel_name} started.")
        try:
            while not self._stop_event.is_set():
                self.connection.process_data_events(time_limit=1)
        except Exception as e:
            print(f"Error in Channel {self.channel_name}: {e}")
        finally:
            self.close()

    def close(self):
        """
        Close the channel and clean up.
        """
        if self.channel:
            self.channel.close()
        print(f"Channel {self.channel_name} closed.")


class ApexMQConnectionManager:
    """
    Manages connections to a RabbitMQ server.

    This class is responsible for establishing and managing connections
    to a RabbitMQ server. It also manages channels within each connection.

    Attributes:
        _connection_list (Dict[str, 'ApexMQConnectionManager']): A class-level dictionary to store all connection instances.
        connection_name (str): The name of the connection.
        connection_params: The parameters for the connection, retrieved from configuration.
        connection (pika.BlockingConnection): The connection object to the RabbitMQ server.
        channel_list (Dict[str, ApexMQChannelManager]): A dictionary to store channels associated with this connection.
    """

    _connection_list: Dict[str, "ApexMQConnectionManager"] = {}

    def __init__(self, connection_name):
        self.connection_name = connection_name
        self.connection_params = get_connection_params(connection_name)
        self.connection: pika.BlockingConnection = None
        self.channel_list: Dict[str, ApexMQChannelManager] = {}
        self.queue_list: Dict[str, ApexMQQueueManager] = {}

    def connect(self):
        """
        Establish a connection to the RabbitMQ server.

        This method uses the connection parameters to create a new BlockingConnection
        to the RabbitMQ server. If the connection fails, it raises a ConnectionError.

        Returns:
            pika.BlockingConnection: The established connection object.

        Raises:
            ConnectionError: If the connection to the RabbitMQ server fails.
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

        self._connection_list[self.connection_name] = self

        return self.connection

    def create_channel(self, channel_name):
        """
        Create a new channel with the given name and add it to the channel list.

        Args:
            channel_name (str): The name of the channel to create.

        Returns:
            ApexMQChannelManager: A new channel manager instance.
        """
        new_channel = ApexMQChannelManager(self.connection, channel_name)
        self.channel_list[channel_name] = new_channel
        return new_channel

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
