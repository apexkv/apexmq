from unittest import TestCase, mock
from pika.exceptions import AMQPConnectionError
from apexmq.connection import ApexMQConnectionManager, ApexMQChannelManager


class TestConnection(TestCase):

    @mock.patch("apexmq.connection.pika.BlockingConnection")
    @mock.patch("apexmq.connection.get_connection_params")
    @mock.patch("apexmq.connection.info")
    def test_successful_connection(
        self, mock_info, mock_get_connection_params, mock_blocking_connection
    ):
        mock_get_connection_params.return_value = {
            "HOST": "localhost",
            "PORT": 5672,
            "USER": "guest",
            "PASSWORD": "guest",
            "VIRTUAL_HOST": "/",
            "MAX_RETRIES": 5,
            "RETRY_DELAY": 5,
            "HEARTBEAT": 60,
            "CONNECTION_TIMEOUT": 10,
        }
        connection = ApexMQConnectionManager("test_connection")
        connection.connect()
        self.assertTrue(connection.connection)
        mock_blocking_connection.assert_called_once()
        mock_info.assert_called_once_with("Connected to RabbitMQ: test_connection")

    @mock.patch("apexmq.connection.pika.BlockingConnection")
    @mock.patch("apexmq.connection.get_connection_params")
    @mock.patch("apexmq.connection.error")
    def test_failed_connection(
        self, mock_error, mock_get_connection_params, mock_blocking_connection
    ):
        mock_get_connection_params.return_value = {
            "HOST": "localhost",
            "PORT": 5672,
            "USER": "guest",
            "PASSWORD": "guest",
            "VIRTUAL_HOST": "/",
            "MAX_RETRIES": 2,
            "RETRY_DELAY": 1,
            "HEARTBEAT": 60,
            "CONNECTION_TIMEOUT": 10,
        }
        mock_blocking_connection.side_effect = AMQPConnectionError()
        connection_manager = ApexMQConnectionManager(connection_name="test")

        with self.assertRaises(ConnectionError):
            connection_manager.connect()

    @mock.patch("apexmq.connection.pika.BlockingConnection")
    @mock.patch("apexmq.connection.get_connection_params")
    @mock.patch("apexmq.connection.info")
    def test_create_channel(self, mock_info, mock_get_params, mock_blocking_connection):
        mock_get_params.return_value = {
            "USER": "testuser",
            "PASSWORD": "testpass",
            "HOST": "localhost",
            "PORT": 5672,
            "VIRTUAL_HOST": "/",
        }

        connection_manager = ApexMQConnectionManager(connection_name="test_connection")
        connection_manager.connect()

        channel = connection_manager.create_channel("test_channel", {})
        self.assertTrue(channel)
        self.assertIsInstance(channel, ApexMQChannelManager)
