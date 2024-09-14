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
        # Fetch RabbitMQ settings
        apexmq_settings = get_apexmq_settings()

        if not apexmq_settings:
            raise ImproperlyConfigured(
                "RabbitMQ connection configurations are not provided."
            )

        # Create connections for each RabbitMQ configuration
        for connection_name, config in apexmq_settings.items():
            # Initialize the connection manager with the settings
            connection_manager = ApexMQConnectionManager(connection_name)

            # Create the connection
            connection_manager.connect()

            # Create and start channels in separate threads
            for channel_name, channel_config in config.get("CHANNELS", {}).items():
                # Create a channel manager (in its own thread)
                channel_manager = connection_manager.create_channel(channel_name)

                # Create queues within this channel
                for queue_name in channel_config.get("QUEUES", {}).keys():
                    queue_manager = ApexMQQueueManager(
                        channel=channel_manager.channel, queue_name=queue_name
                    )

                    # Start consuming messages on the queue
                    queue_manager.start_consuming(self.message_callback)

    def message_callback(self, channel, method, properties, body):
        """
        Callback function to process incoming messages from the queue.
        Customize this method to fit your use case for handling messages.
        """
        print(f"Received message: {body}")
