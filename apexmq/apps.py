import threading
import time
import importlib
from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured
from django.utils.autoreload import autoreload_started

from .conf import get_connection_settings, get_consumers_from_apps, info, warning, error
from .consumers import action_handlers
from .connection import (
    ApexMQConnectionManager,
    ApexMQQueueManager,
)

thread_list = []


class ApexMQConfig(AppConfig):
    name = "apexmq"
    label = "ApexMQ"

    def ready(self):
        """
        Called when Django starts. If in DEBUG mode, sets up the autoreload
        listener to monitor code changes and reconfigure RabbitMQ connections.
        """
        from django.conf import settings

        self.autodiscover_consumers(settings)

        self.register_on_consume_handlers()

        if settings.DEBUG:
            self.watch_for_changes()
        else:
            self.setup_rabbitmq()

    def watch_for_changes(self):
        """
        Connects the `setup_rabbitmq` method to the `autoreload_started` signal.
        This method will be called whenever Django detects a code change.
        """
        autoreload_started.connect(self.setup_rabbitmq)

    def setup_rabbitmq(self, sender=None, **kwargs):
        pass

    def message_callback(self, channel, method, properties, body):
        pass

    def register_on_consume_handlers(self):
        for action, handler in action_handlers.items():
            pass

    def autodiscover_consumers(self, settings):
        """
        Automatically discovers and imports consumers from all installed apps.
        This looks for a `consumers.py` file in each app listed in `INSTALLED_APPS`.
        """
        for app in settings.INSTALLED_APPS:
            if app != "apexmq":
                try:
                    # Dynamically import the consumers module from each installed app
                    importlib.import_module(f"{app}.consumers")
                except ModuleNotFoundError:
                    # If the app doesn't have a consumers module, skip it
                    pass

    def log_details(self, action, queue):
        info(f'"CONSUMED - QUEUE: {queue} | ACTION: {action}"')
