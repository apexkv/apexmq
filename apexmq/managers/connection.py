import time, pika
from pika.adapters.blocking_connection import BlockingConnection
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from apexmq.conf import Logger, get_connection_settings


class ApexMQConnectionManager:
    """
    A class to manage the connection to RabbitMQ.

    Attributes:
        connection (BlockingConnection): The connection object to RabbitMQ.
        params (ApexMQSettingsConnection): The connection settings.
        credentials (pika.PlainCredentials): The credentials to authenticate with Rabbit

    Methods:
        connect: Establishes a connection to RabbitMQ.    
    """
    def __init__(self):
        self.connection:BlockingConnection|None = None
        self.params = get_connection_settings()
        self.credentials:pika.PlainCredentials|None = None

    def set_credentials(self):
        """
        Sets the credentials to authenticate with RabbitMQ.

        Notes:
            - The credentials are set using the PlainCredentials class from the pika library.
        """
        if not self.params:
            raise ImproperlyConfigured("RabbitMQ settings are required.")

        if not self.params.user or not self.params.password:
            raise ImproperlyConfigured("RabbitMQ user and password are required.")
        
        self.credentials = pika.PlainCredentials(
            self.params.user,
            self.params.password,
        )

    def connect(self):
        """
        Establishes a connection to RabbitMQ.

        Raises:
            ImproperlyConfigured: If the connection could not be established after multiple retries.
        
        Notes:
            - The connection is established using the BlockingConnection class from the pika library.
            - The connection parameters are fetched from the APEXMQ settings.
            - The connection is retried multiple times in case of failure.
        """
        if self.connection and self.connection.is_open:
            Logger.info("Connection to RabbitMQ already established.")
            return
        
        self.set_credentials()

        retries = self.params.retries
        WAIT_TIME = 3
        while retries > 0:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        self.params.host,
                        credentials=self.credentials,
                        heartbeat=0
                    )
                )
                self.connection = connection
                Logger.info("Successfully connected to RabbitMQ.")
                break
            except AMQPConnectionError as e:
                Logger.error(f"Failed to connect to RabbitMQ: {e}. Retrying in {WAIT_TIME} seconds...")
            except Exception as e:
                Logger.error(f"Unexpected error: {e}")
            retries -= 1
            time.sleep(WAIT_TIME)

        if retries == 0 and self.connection is None:
            raise ImproperlyConfigured("Could not establish a RabbitMQ connection after multiple retries.")

    def close(self):
        """
        Closes the connection to RabbitMQ.

        Notes:
            - The connection is closed using the close() method of the BlockingConnection class.
        """
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
                Logger.info("Connection to RabbitMQ closed.")
            except Exception as e:
                Logger.error(f"Error closing RabbitMQ connection: {e}")
            finally:
                self.connection = None
        else:
            Logger.info("No connection to close.")