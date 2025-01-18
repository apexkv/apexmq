from unittest import TestCase
from unittest.mock import patch, MagicMock
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from apexmq.managers import ApexMQConnectionManager


class TestApexMQConnectionManager(TestCase): 
    @patch("apexmq.managers.connection.pika.BlockingConnection")
    def test_connect_success(self, mock_blocking_connection):
        mock_blocking_connection.return_value = MagicMock(is_open=False)

        manager = ApexMQConnectionManager()
        manager.params.retries = 1

        manager.connect()

        self.assertIsNotNone(manager.params)
        self.assertIsNotNone(manager.credentials)

        self.assertIsNotNone(manager.connection)
        mock_blocking_connection.assert_called_once()

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    @patch("apexmq.managers.connection.Logger")
    def test_connect_failure_with_more_retries(self, mock_logger, mock_blocking_connection):
        mock_blocking_connection.side_effect = AMQPConnectionError("Connection error")

        RETRIES = 2
        manager = ApexMQConnectionManager()
        manager.params.retries = RETRIES 

        with self.assertRaises(ImproperlyConfigured) as context:
            manager.connect()

        # Ensure the logger was called RETRIES times
        self.assertEqual(mock_logger.error.call_count, RETRIES)
        mock_logger.error.assert_any_call("Failed to connect to RabbitMQ: Connection error. Retrying in 3 seconds...")
        self.assertEqual(str(context.exception), "Could not establish a RabbitMQ connection after multiple retries.")

        self.assertIsNone(manager.connection)

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    @patch("apexmq.managers.connection.Logger")
    def test_connect_failure_due_to_invalid_credentials(self, mock_logger, mock_blocking_connection):
        mock_blocking_connection.side_effect = AMQPConnectionError("Authentication error")

        manager = ApexMQConnectionManager()

        RETRIES = 2

        manager.params.user = "wrong_user"
        manager.params.password = "wrong_password"
        manager.params.retries = RETRIES

        with self.assertRaises(ImproperlyConfigured) as context:
            manager.connect()

        # The logger should have been called twice
        self.assertEqual(mock_logger.error.call_count, RETRIES)
        mock_logger.error.assert_any_call("Failed to connect to RabbitMQ: Authentication error. Retrying in 3 seconds...")
        self.assertEqual(str(context.exception), "Could not establish a RabbitMQ connection after multiple retries.")

        self.assertIsNone(manager.connection)

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    @patch("apexmq.managers.connection.Logger")
    def test_connect_when_connection_is_already_open(self, mock_logger, mock_blocking_connection):
        mock_open_connection = MagicMock(is_open=True)
        mock_blocking_connection.return_value = mock_open_connection

        manager = ApexMQConnectionManager()

        manager.connection = mock_open_connection

        manager.connect()

        self.assertIs(manager.connection, mock_open_connection)
        mock_blocking_connection.assert_not_called()

        mock_logger.info.assert_called_once_with("Connection to RabbitMQ already established.")
    
    @patch("apexmq.managers.connection.pika.BlockingConnection")
    def test_connect_when_params_are_not_set(self, mock_blocking_connection):
        manager = ApexMQConnectionManager()

        manager.params = None

        with self.assertRaises(ImproperlyConfigured) as context:
            manager.connect()

        # Ensure the logger was called once
        self.assertEqual(str(context.exception), "RabbitMQ settings are required.")
        self.assertIsNone(manager.connection)
        self.assertIsNone(manager.credentials)
        mock_blocking_connection.assert_not_called()

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    def test_close_connection_success(self, mock_blocking_connection):
        mock_open_connection = MagicMock(is_open=True)
        mock_blocking_connection.return_value = mock_open_connection

        manager = ApexMQConnectionManager()

        manager.connection = mock_open_connection

        manager.close()

        self.assertIsNone(manager.connection)
        mock_open_connection.close.assert_called_once()

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    @patch("apexmq.managers.connection.Logger")
    def test_close_connection_when_connection_is_none(self, mock_logger, mock_blocking_connection):
        manager = ApexMQConnectionManager()

        manager.connection = None

        manager.close()

        mock_blocking_connection.assert_not_called()
        mock_logger.info.assert_called_once_with("No connection to close.")

    @patch("apexmq.managers.connection.pika.BlockingConnection")
    @patch("apexmq.managers.connection.Logger")
    def test_close_connection_when_connection_is_not_open(self, mock_logger, mock_blocking_connection):
        mock_closed_connection = MagicMock(is_open=False)
        mock_blocking_connection.return_value = mock_closed_connection

        manager = ApexMQConnectionManager()

        manager.connection = mock_closed_connection

        manager.close()

        mock_blocking_connection.assert_not_called()
        mock_logger.info.assert_called_once_with("No connection to close.") 