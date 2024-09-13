import pika
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
    _connection_list = {}

    def __init__(self, connection_name):
        self.connection_name = connection_name
        self.connection_params = get_connection_params(connection_name)
        self.connection = None
        self.channel_list = {}
