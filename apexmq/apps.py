import logging
import threading
from django.utils import timezone
from django.apps import AppConfig
from django.core.exceptions import ImproperlyConfigured
from django.utils.autoreload import autoreload_started

from .conf import get_apexmq_settings, get_consumers_from_apps
from .connection import (
    ApexMQConnectionManager,
    ApexMQQueueManager,
)

thread_list = []
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class ApexMQConfig(AppConfig):
    name = "apexmq"
    label = "ApexMQ"

    def ready(self):
        """
        Called when Django starts. If in DEBUG mode, sets up the autoreload
        listener to monitor code changes and reconfigure RabbitMQ connections.
        """
        from django.conf import settings

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
                channel_manager = connection_manager.create_channel(channel_name)

                for queue_name in channel_config.get("QUEUES", {}).keys():
                    queue_manager = ApexMQQueueManager(
                        channel=channel_manager.channel, queue_name=queue_name
                    )

                    # Set up message consummer for each queue
                    channel_manager.channel.basic_consume(
                        queue=queue_name,
                        on_message_callback=self.message_callback,
                        auto_ack=True,
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

        # Iterate over all registered consumers
        for ConsumerClass in consumers:
            # Check if the action type matches the consumer's lookup prefix
            if ConsumerClass.lookup_prefix == action_type.split(".")[0]:
                ConsumerClass().process_messege(action_type, body)

        if len(consumers) == 0:
            msg = f"No consumers found for the action type: {action_type}"
            logger.warning(msg)

    def log_details(self, action, queue):
        timestamp = timezone.now()
        details = f'[{timestamp.day:02d}/{timestamp.month:02d}/{timestamp.year} {timestamp.hour:02d}:{timestamp.minute:02d}:{timestamp.second:02d}] "QUEUE: {queue} | ACTION: {action}"'
        logger.info(details)
