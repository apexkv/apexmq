from unittest import TestCase
from unittest.mock import patch, MagicMock
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured
from apexmq.managers.consumer import ApexMQConsumerManager


class TestApexMQConsumerManager(TestCase):
    @patch('apexmq.managers.consumer.ApexMQConnectionManager')
    @patch('apexmq.managers.consumer.get_connection_settings')
    @patch('apexmq.managers.consumer.get_consumers_from_apps')
    def setUp(self, mock_get_consumers, mock_get_settings, mock_connection):
        self.manager = ApexMQConsumerManager()
        self.manager.connection.connect = MagicMock()
        self.manager.channel = MagicMock()
        self.manager.queue_params = {'test': MagicMock()}
        self.manager.consumers = {'test': MagicMock()}

    def test_init_success(self):
        self.assertIsInstance(self.manager.channel, MagicMock)
        self.assertIsInstance(self.manager.queue_params, dict)
        self.assertIsInstance(self.manager.consumers, dict)
    
    def test_connect_success(self):
        self.manager.connect()
        self.manager.connection.connect.assert_called_once()

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

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.ready()

        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_ready_failure_when_connection_is_not_open(self):
        self.manager.connection.connection = MagicMock(is_open=False)

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.ready()

        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_create_channel_success(self):
        self.manager.connection.connection = MagicMock(is_open=True)

        self.manager.create_channel()

        self.assertIsNotNone(self.manager.channel)
        self.assertTrue(self.manager.channel.is_open)

    def test_create_channel_failure_when_connection_is_none(self):
        self.manager.connection.connection = None
        self.manager.channel = None

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.create_channel()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ connection is not established.")

    def test_declare_queues_success(self):
        self.manager.channel = MagicMock()

        self.manager.declare_queues()

        self.manager.channel.queue_declare.assert_called_once()

    def test_declare_queues_failure_when_channel_is_none(self):
        self.manager.channel = None

        with self.assertRaises(ImproperlyConfigured) as context:
            self.manager.declare_queues()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ channel is not established.")

    def test_start_consuming_success(self):
        self.manager.channel = MagicMock()

        self.manager.start_consuming()

        self.manager.channel.start_consuming.assert_called_once()

    def test_start_consuming_failure_when_channel_is_none(self):
        self.manager.channel = None

        with self.assertRaises(AMQPConnectionError) as context:
            self.manager.start_consuming()

        self.assertIsNone(self.manager.channel)
        self.assertEqual(str(context.exception), "RabbitMQ channel is not established.")

    def test_close_success(self):
        self.manager.channel = MagicMock()

        self.manager.close()

        self.manager.channel.close.assert_called_once()

    @patch("apexmq.managers.consumer.Logger")
    def test_close_failure_when_channel_is_none(self, mock_logger):
        self.manager.channel = None

        self.manager.close()

        self.assertIsNone(self.manager.channel)
        mock_logger.error.assert_any_call("RabbitMQ channel is not established.")      

    @patch("apexmq.managers.consumer.Logger")
    def test_close_failure_when_channel_is_closed(self, mock_logger):
        self.manager.channel = MagicMock(is_open=False)

        self.manager.close()

        self.assertFalse(self.manager.channel.is_open)
        mock_logger.error.assert_any_call("RabbitMQ channel is not established.")

    @patch("apexmq.managers.consumer.Logger")
    def test_close_failure_when_connection_is_none(self, mock_logger):
        self.manager.connection = None

        self.manager.close()

        self.assertIsNone(self.manager.connection)
        mock_logger.error.assert_any_call("Consumer connection is already closed.")