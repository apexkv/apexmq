import json
import pika
import time
import threading
from typing import Dict, List
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured
import requests
from requests.auth import HTTPBasicAuth

from .conf import get_connection_params, info, error


class ApexMQExchangeManager:
    """
    Manages the configuration and operations of an exchange in the ApexMQ messaging system.
    Attributes:
        channel: The communication channel to be used for the exchange.
        exchange_name: The name of the exchange.
        exchange_config: The configuration settings for the exchange.
        _exchange_list (Dict[str, "ApexMQExchangeManager"]): A class-level dictionary to keep track of all exchange instances.
    """

    _exchange_list: Dict[str, "ApexMQExchangeManager"] = {}

    def __init__(
        self, channel: BlockingChannel, exchange_name: str, exchange_config: dict
    ):
        """
        Initializes the ApexMQExchangeManager.

        Args:
            channel (BlockingChannel): The channel used to interact with RabbitMQ.
            exchange_name (str): The name of the exchange.
            exchange_config (dict): The configuration settings for the exchange.
        """
        self.channel = channel
        self.exchange_name = exchange_name
        self.exchange_config = exchange_config


class ApexMQQueueManager:
    """
    Manages a specific queue in RabbitMQ.

    Attributes:
        channel (BlockingChannel): The channel used to interact with RabbitMQ.
        queue_name (str): The name of the queue.
        queue (pika.Queue): The declared queue instance.
        _queue_list (Dict[str, "ApexMQQueueManager"]): A class-level dictionary to keep track of all queue instances.
    """

    _queue_list: Dict[str, "ApexMQQueueManager"] = {}

    def __init__(self, channel: BlockingChannel, queue_name: str, queue_config: dict):
        """
        Initializes the ApexMQQueueManager.

        Args:
            channel (BlockingChannel): The channel used to interact with RabbitMQ.
            queue_name (str): The name of the queue.
        """
        self.channel = channel
        self.queue_name = queue_name
        self.queue_config = queue_config
        # Queue params
        self.__AUTO_ACK__: bool = queue_config.get("AUTO_ACK", True)
        self.__AUTO_DELETE__: bool = queue_config.get("AUTO_DELETE", False)
        self.__DURABLE__: bool = queue_config.get("DURABLE", False)
        self.__EXCLUSIVE__: bool = queue_config.get("EXCLUSIVE", False)
        self.__PASSIVE__: bool = queue_config.get("PASSIVE", False)
        self.declare_queue()

    def declare_queue(self):
        """
        Declares the queue in RabbitMQ.
        """
        self.queue = self.channel.queue_declare(
            queue=self.queue_name,
            auto_delete=self.__AUTO_DELETE__,
            durable=self.__DURABLE__,
            exclusive=self.__EXCLUSIVE__,
            passive=self.__PASSIVE__,
        )
        self._queue_list[self.queue_name] = self
        info(f"Queue created: {self.queue_name}")

    @classmethod
    def get_queue(cls, queue_name: str):
        if queue_name not in cls._queue_list:
            raise ImproperlyConfigured(
                f"Invalid queue name. Your choices are {list(cls._queue_list.keys())}"
            )
        return cls._queue_list[queue_name]

    def basic_consumer(self, on_message_callback):
        """
        Consumes messages from the queue.

        Args:
            on_message_callback (Callable): The callback function to handle incoming messages.
        """
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message_callback,
            auto_ack=self.__AUTO_ACK__,
        )


class ApexMQChannelManager:
    """
    Manages a specific channel in RabbitMQ.

    Attributes:
        connection (pika.BlockingConnection): The connection to RabbitMQ.
        channel_name (str): The name of the channel.
        channel (BlockingChannel): The created channel instance.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to keep track of all queues in this channel.
    """

    _channels_list: Dict[str, "ApexMQChannelManager"] = {}
    _publish_channel: "ApexMQChannelManager" = None

    def __init__(
        self,
        connection_manager: "ApexMQConnectionManager",
        channel: BlockingChannel,
        channel_name: str,
        channel_config: dict,
    ):
        """
        Initializes the ApexMQChannelManager.

        Args:
            connection (pika.BlockingConnection): The connection to RabbitMQ.
            channel_name (str): The name of the channel.
        """
        self.connection_manager = connection_manager
        self.channel = channel
        self.channel_name = channel_name
        self.channel_config = channel_config
        self.queue_list: Dict[str, ApexMQQueueManager] = {}
        self.publish_channel = connection_manager.get_connection().channel()
        self._publish_channel = self

    @classmethod
    def get_publish_channel(cls):
        return cls._publish_channel

    @classmethod
    def get_channel(cls, channel_name):
        if channel_name not in cls._channels_list:
            raise ImproperlyConfigured(
                f"Invalid queue name. Your choices are {list(cls._channels_list.keys())}"
            )
        return cls._channels_list[channel_name]

    @classmethod
    def get_first_channel_name(cls):
        return list(cls._channels_list.keys())[0]

    def create_queue(self, queue_name: str):
        """
        Creates a queue in the channel.

        Args:
            queue_name (str): The name of the queue to create.
        """
        queue_config = self.channel_config.get("QUEUES", {}).get(queue_name, {})
        if queue_name not in self.queue_list:
            queue_manager = ApexMQQueueManager(self.channel, queue_name, queue_config)
            self.queue_list[queue_name] = queue_manager
            return queue_manager
        return self.queue_list[queue_name]

    def publish(self, action: str, body: dict, to: str):
        """
        Publishes a message to the specified queue.

        Args:
            action (str): The action type of the message.
            body (dict): The message body.
            to (str): The name of the queue to publish the message to.
        """
        properties = pika.BasicProperties(content_type=action)
        try:
            self.publish_channel.basic_publish(
                exchange="",
                routing_key=to,
                body=json.dumps(body),
                properties=properties,
            )
        except Exception as e:
            error(f"Failed to publish message to {to}: {e}")


class ApexMQConnectionManager:
    """
    Manages the connection to RabbitMQ.

    Attributes:
        connection_name (str): The name of the connection configuration.
        connection_params (dict): Parameters used to establish the connection.
        connection (pika.BlockingConnection): The connection to RabbitMQ.
        channel_list (Dict[str, ApexMQChannelManager]): A dictionary to keep track of all channels in this connection.
        queue_list (Dict[str, ApexMQQueueManager]): A dictionary to keep track of all queues across channels.
    """

    def __init__(self, connection_name: str):
        """
        Initializes the ApexMQConnectionManager.

        Args:
            connection_name (str): The name of the connection configuration.
        """
        self.connection_name = connection_name
        self.local = threading.local()
        self.connection_params = get_connection_params(connection_name)
        self.connection: pika.BlockingConnection = None
        self.channel_list: Dict[str, ApexMQChannelManager] = {}
        self.queue_list: Dict[str, ApexMQQueueManager] = {}
        # User credentials
        self.__USER__: str = self.connection_params["USER"]
        self.__PASSWORD__: str = self.connection_params["PASSWORD"]
        # Set default values
        self.__PORT__: int = self.connection_params.get("PORT", 5672)
        self.__HOST__: str = self.connection_params.get("HOST", "localhost")
        self.__VIRTUAL_HOST__: str = self.connection_params.get("VIRTUAL_HOST", "/")
        self.__MAX_RETRIES__: int = self.connection_params.get("MAX_RETRIES", 5)
        self.__RETRY_DELAY__: int = self.connection_params.get("RETRY_DELAY", 5)
        self.__HEARTBEAT__: int = self.connection_params.get("HEARTBEAT", 60)
        self.__CONNECTION_TIMEOUT__: int = self.connection_params.get(
            "CONNECTION_TIMEOUT", 10
        )

    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Returns:
            pika.BlockingConnection: The established connection instance.

        Raises:
            ConnectionError: If unable to connect to RabbitMQ.
        """
        credentials = pika.PlainCredentials(
            username=self.__USER__,
            password=self.__PASSWORD__,
        )

        for _ in range(self.__MAX_RETRIES__):
            try:
                connection_params = pika.ConnectionParameters(
                    host=self.__HOST__,
                    port=self.__PORT__,
                    virtual_host=self.__VIRTUAL_HOST__,
                    credentials=credentials,
                    heartbeat=self.__HEARTBEAT__,
                    blocked_connection_timeout=self.__CONNECTION_TIMEOUT__,
                )
                connection = pika.BlockingConnection(connection_params)
                info(
                    f"Connected to RabbitMQ: {self.connection_name} (Thread: {threading.current_thread().name})"
                )
                return connection
            except AMQPConnectionError as e:
                error(
                    f"Failed to connect to message queue server: {self.connection_name}. Retrying..."
                )
                time.sleep(self.__RETRY_DELAY__)

        raise ConnectionError(
            f"Failed to connect to message queue server after {self.__MAX_RETRIES__} attempts"
        )

    def create_channel(
        self, channel_name: str, channel_config: dict
    ) -> ApexMQChannelManager:
        """
        Creates and returns a channel manager for the specified channel name.

        Args:
            channel_name (str): The name of the channel to create.

        Returns:
            ApexMQChannelManager: The created channel manager.

        Raises:
            Exception: If the connection is not established.
        """
        connection = self.get_connection()
        channel = connection.channel()
        channel_manager = ApexMQChannelManager(
            self, channel, channel_name, channel_config
        )
        info(
            f"Channel {channel_name} created (Thread: {threading.current_thread().name})."
        )
        return channel_manager

    def close_connection(self):
        """
        Closes the connection to RabbitMQ.
        """
        if hasattr(self.local, "connection") and self.local.connection:
            self.local.connection.close()
            self.local.connection = None
            info(
                f"RabbitMQ connection closed (Thread: {threading.current_thread().name})"
            )

    def get_queue_list_in_connection(self) -> List[str]:
        url = f"http://{self.__HOST__}:{self.__PORT__}/api/queues"
        response = requests.get(
            url, auth=HTTPBasicAuth(self.__USER__, self.__PASSWORD__)
        )

        if response.status_code == 200:
            queues = response.json()
            queues = [queue["name"] for queue in queues]
            return queues
        else:
            print(f"Failed to fetch queues: {response.status_code}")
            return []

    def get_connection(self):
        if (
            not hasattr(self.local, "connection")
            or self.local.connection is None
            or self.local.connection.is_closed
        ):
            self.local.connection = self.connect()
        return self.local.connection
