from unittest import TestCase, mock
from apexmq.connection import (
    ApexMQConnectionManager,
)


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
