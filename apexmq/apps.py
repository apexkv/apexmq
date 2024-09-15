from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured

from .conf import get_apexmq_settings
from .connection import (
    ApexMQConnectionManager,
    ApexMQChannelManager,
    ApexMQQueueManager,
)


class ApexMQConfig(AppConfig):
    name = "ApexMQ"
    label = "apexmq"

    def ready(self):
        pass

    def message_callback(self, channel, method, properties, body):
        """
        Callback function to process incoming messages from the queue.
        Customize this method to fit your use case for handling messages.
        """
        print(f"Received message: {body}")
