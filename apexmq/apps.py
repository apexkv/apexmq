import threading
import importlib
from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured
from django.utils.autoreload import autoreload_started

from .conf import get_apexmq_settings, get_consumers_from_apps, info, warning
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

    def setup_rabbitmq(self, sender, **kwargs):
        """
        Sets up RabbitMQ connections and channels. This method is called
        when Django detects a code change in DEBUG mode.
        """
        global thread_list
        # Stop any existing RabbitMQ consumer threads
        self.stop_threads()
        # Fetch RabbitMQ settings
        apexmq_settings = get_apexmq_settings()

        if not apexmq_settings:
            raise ImproperlyConfigured(
                "RabbitMQ connection configurations are not provided."
            )

        # Iterate over all RabbitMQ connection configurations
        for connection_name, config in apexmq_settings.items():
            connection_manager = ApexMQConnectionManager(connection_name)
            connection_manager.connect()

            # Iterate over all channels and queues in the configuration
            for channel_name, channel_config in config.get("CHANNELS", {}).items():
                channel_manager = connection_manager.create_channel(
                    channel_name, channel_config
                )

                for queue_name, queue_config in channel_config.get(
                    "QUEUES", {}
                ).items():
                    queue_manager = ApexMQQueueManager(
                        channel=channel_manager.channel,
                        queue_name=queue_name,
                        queue_config=queue_config,
                    )

                    queue_manager.basic_consumer(
                        on_message_callback=self.message_callback
                    )

                # Start a new thread to handle message consumption
                thread = threading.Thread(
                    target=channel_manager.channel.start_consuming, daemon=True
                )
                thread_list.append(thread)
                thread.start()

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
        """
        Callback function to process messages from RabbitMQ queues.
        This function matches the action type from the message properties with
        the registered consumers and delegates message processing to the
        appropriate consumer class.
        """
        action_type = str(properties.content_type)
        self.log_details(action_type, method.routing_key)
        # Fetch registered consumer classes
        consumers = get_consumers_from_apps()

        action_method_found = False

        # Iterate over all registered consumers
        for ConsumerClass in consumers:
            # Check if the action type matches the consumer's lookup prefix
            if ConsumerClass.lookup_prefix == action_type.split(".")[0]:
                ConsumerClass().process_messege(action_type, body)
                action_method_found = True
                break

        if not action_method_found:
            if action_type in action_handlers:
                action_handlers[action_type](body)
                action_method_found = True

        if not action_method_found:
            msg = f"No consumers found for the action type: {action_type}"
            warning(msg)

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
