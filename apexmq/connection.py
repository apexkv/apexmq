import pika
from django.conf import settings


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
