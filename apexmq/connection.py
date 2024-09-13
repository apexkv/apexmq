import pika
from typing import Dict
from django.conf import settings
from .conf import get_connection_params


class ApexMQQueManager:
    _que_list = {}

    def __init__(self, channel, que_name):
        self.channel = channel
        self.que_name = que_name
        self.que = None


class ApexMQChannelManager:
    _channel_list = {}

    def __init__(self, connection, channel_name):
        self.connection = connection
        self.channel_name = channel_name
        self.channel = None


class ApexMQConnectionManager:
    _connection_list: Dict[str, pika.BlockingConnection] = {}

    def __init__(self, connection_name):
        self.connection_name = connection_name
        self.connection_params = get_connection_params(connection_name)
        self.connection: pika.BlockingConnection = None
        self.channel_list = {}

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

        self._connection_list[self.connection_name] = self.connection

        return self.connection
