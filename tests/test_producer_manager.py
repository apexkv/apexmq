from unittest import TestCase
from unittest.mock import patch, MagicMock
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured
from apexmq.managers import ApexMQProducerManager


class TestApexMQProducerManager(TestCase):

    def setUp(self):
        self.manager = ApexMQProducerManager
        self.manager.connection = MagicMock()
        self.manager.connection.connection = MagicMock()
        self.manager.channel = None

    def test_connect_success(self):
        self.manager.connection.connection = MagicMock(is_open=True)

        self.manager.connect()

        self.assertIsNotNone(self.manager.connection.connection)
        self.assertTrue(self.manager.connection.connection.is_open)

    def test_connect_failure_when_connection_manager_none(self):
        self.manager.connection = None

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.connect()

        self.assertIsNone(self.manager.connection)
        self.assertEqual(str(context.exception), "Connection manager is not established.")
    
    def test_ready_success(self):
        self.manager.connection.connection = MagicMock(is_open=True)

        self.manager.ready()

        self.assertIsNotNone(self.manager.channel)
        self.assertTrue(self.manager.channel.is_open)

    def test_ready_failure_when_connection_is_none(self):
        self.manager.connection.connection = None

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.ready()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_ready_failure_when_connection_is_not_open(self):
        self.manager.connection.connection = MagicMock(is_open=False)

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.ready()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ connection is not open.")

    def test_create_channel_success(self):
        self.manager.connection.connection = MagicMock(is_open=True)

        self.manager.create_channel()

        self.assertIsNotNone(self.manager.channel)
        self.assertTrue(self.manager.channel.is_open)

    def test_create_channel_failure_when_connection_is_none(self):
        self.manager.connection.connection = None

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.create_channel()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_create_channel_failure_when_connection_is_not_open(self):
        self.manager.connection.connection = MagicMock(is_open=False)

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.create_channel()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ connection is not open.")
    
    @patch("apexmq.managers.producer.Logger")
    def test_publish_success(self, mock_logger):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = MagicMock()

        to = "queue"
        action = "action"

        self.manager.publish(action, {}, to)

        self.manager.channel.basic_publish.assert_called_once()
        mock_logger.info.assert_called_once_with(f'"PUBLISHED - QUEUE: {to} | ACTION: {action}"')

    def test_publish_failure_when_connection_is_none(self):
        self.manager.connection.connection = None
        self.manager.channel = MagicMock()

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.publish("action", {}, "queue")

        self.manager.channel.basic_publish.assert_not_called()
        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_publish_failure_when_channel_is_none(self):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = None

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.publish("action", {}, "queue")

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ channel is not established.")

    @patch("apexmq.managers.producer.Logger")
    def test_publish_failure_when_publish_fails(self, mock_logger):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = MagicMock()

        to = "queue"
        e = "Error"
        action = "action"

        self.manager.channel.basic_publish.side_effect = Exception(e)

        self.manager.publish(action, {}, to)

        self.manager.channel.basic_publish.assert_called_once()
        mock_logger.info.assert_not_called()
        mock_logger.error.assert_called_once_with(f"Failed to publish message to {to}: {e}")

    @patch("apexmq.managers.producer.Logger")
    def test_close_success(self, mock_logger):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = MagicMock()

        self.manager.close()

        mock_logger.debug.assert_any_call("Closed producer channel and connection.")

    @patch("apexmq.managers.producer.Logger")
    def test_close_failure_when_channel_is_none(self, mock_logger):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = None

        self.manager.close()

        mock_logger.debug.assert_any_call("Producer channel is already closed.")

    @patch("apexmq.managers.producer.Logger")
    def test_close_failure_when_connection_is_none(self, mock_logger):
        self.manager.connection.connection = None
        self.manager.channel = MagicMock()

        self.manager.close()

        mock_logger.debug.assert_any_call("Producer connection is already closed.")

    @patch("apexmq.managers.producer.Logger")
    def test_close_failure_in_unknown_exception(self, mock_logger):
        self.manager.connection.connection = MagicMock(is_open=True)
        self.manager.channel = MagicMock()

        e = "Error"
        self.manager.channel.close.side_effect = Exception(e)

        self.manager.close()

        mock_logger.error.assert_any_call(f"Error closing producer: {e}")