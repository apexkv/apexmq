import pika
from typing import Dict
from django.conf import settings
from .conf import get_connection_params
from pika.adapters.blocking_connection import BlockingChannel


class ApexMQQueManager:
    _que_list: Dict[str, "ApexMQQueManager"] = {}

    def __init__(self, channel: BlockingChannel, que_name):
        self.channel = channel
        self.que_name = que_name
        self.que = channel.queue_declare(queue=que_name)
        self._que_list[self.que_name] = self


class ApexMQChannelManager:
    _channel_list: Dict[str, "ApexMQChannelManager"] = {}

    def __init__(self, connection: pika.BlockingConnection, channel_name):
        self.connection = connection
        self.channel_name = channel_name
        self.channel = connection.channel()
        self.que_list: Dict[str, ApexMQQueManager] = {}
        self._channel_list[self.channel_name] = self

    def create_que(self, que_name):
        """
        Create a new queue with the given name and add it to the queue list.

        This method creates a new ApexMQQueManager instance, which in turn
        declares a new queue on the RabbitMQ server.

        Args:
            que_name (str): The name of the queue to create.

        Returns:
            ApexMQQueManager: A new queue manager instance.
        """
        new_que = ApexMQQueManager(self.channel, que_name)
        self.que_list[que_name] = new_que
        return new_que


class ApexMQConnectionManager:
    _connection_list: Dict[str, "ApexMQConnectionManager"] = {}

    def __init__(self, connection_name):
        self.connection_name = connection_name
        self.connection_params = get_connection_params(connection_name)
        self.connection: pika.BlockingConnection = None
        self.channel_list: Dict[str, ApexMQChannelManager] = {}

    def connect(self):
        """
        Establish a connection.
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
        except Exception as e:
            raise ConnectionError(f"Failed to connect to messege que server: {e}")

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
