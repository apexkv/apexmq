import pika
from django.conf import settings


class ApexMQQueManager:
    _que_list = {}

    def __init__(self, channel, que_name):
        self.channel = channel
        self.que_name = que_name
        self.que = None
