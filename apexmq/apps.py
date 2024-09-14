from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured

from .conf import get_apexmq_settings
from .connection import ApexMQConnectionManager


class ApexMQConfig(AppConfig):
    name = "ApexMQ"
    label = "apexmq"

    def ready(self):
        apexmq_settings = get_apexmq_settings()

        if len(apexmq_settings) == 0:
            raise ImproperlyConfigured(
                "Haven't configured rabbitmq connection configurations."
            )

        for connection_name, _ in apexmq_settings.items():
            new_connection = ApexMQConnectionManager(connection_name)
            new_connection.create_all_channels_and_queues()
            new_connection.start_consuming()
