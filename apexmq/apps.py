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
            # Set up autoreload for development
            self.watch_for_changes()
        else:
            # Set up RabbitMQ connections directly for production
            self.setup_rabbitmq()

    def watch_for_changes(self):
        """
        Connects the `setup_rabbitmq` method to the `autoreload_started` signal.
        This method will be called whenever Django detects a code change.
        """
        autoreload_started.connect(self.setup_rabbitmq)

    def setup_rabbitmq(self, sender=None, **kwargs):
        apexmq_settings = get_connection_settings()

        if not apexmq_settings:
            raise ImproperlyConfigured(
                "RabbitMQ connection configurations are not provided."
            )

        for connection_name, config in apexmq_settings.items():
            connection_manager = ApexMQConnectionManager(connection_name)

            for channel_name, channel_config in config.get("CHANNELS", {}).items():
                for queue_name, queue_config in channel_config.get(
                    "QUEUES", {}
                ).items():
                    thread = threading.Thread(
                        target=self.consume_queue,
                        args=(
                            connection_manager,
                            channel_name,
                            channel_config,
                            queue_name,
                            queue_config,
                        ),
                        daemon=True,
                    )
                    thread.start()

    def consume_queue(
        self, connection_manager, channel_name, channel_config, queue_name, queue_config
    ):
        while True:
            try:
                channel_manager = connection_manager.create_channel(
                    channel_name, channel_config
                )
                queue_manager = ApexMQQueueManager(
                    channel=channel_manager.channel,
                    queue_name=queue_name,
                    queue_config=queue_config,
                )
                queue_manager.basic_consumer(on_message_callback=self.message_callback)
                channel_manager.channel.start_consuming()
            except Exception as e:
                error(f"Error in consumer thread for queue {queue_name}: {e}")
                time.sleep(5)

    def stop_threads(self):
        """
        Stops all running consumer threads by joining them.
        """
        global thread_list
        for thread in thread_list:
            if thread.is_alive():
                thread.join(timeout=1)
        thread_list = []

    def message_callback(self, channel, method, properties, body):
        action_type = str(properties.content_type)
        self.log_details(action_type, method.routing_key)
        consumers = get_consumers_from_apps()

        action_method_found = False

        for ConsumerClass in consumers:
            if ConsumerClass.lookup_prefix == action_type.split(".")[0]:
                ConsumerClass().process_message(action_type, body)
                action_method_found = True
                break

        if not action_method_found:
            if action_type in action_handlers:
                action_handlers[action_type](body)
                action_method_found = True

        if not action_method_found:
            warning(f"No consumers found for the action type: {action_type}")

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
