import json
from unittest import TestCase
from unittest.mock import patch, MagicMock
from apexmq.producers import publish, on_model_action
from django.db.models.signals import post_save, post_delete
from django.db import models

# Mock model for testing
class MockModel(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "testapp"


class TestProducers(TestCase):

    @patch("apexmq.producers.ApexMQProducerManager.publish")
    def test_publish_to_queues(self, mock_publish):
        """Test the `publish` function publishes messages to specified queues."""
        action = "user.create"
        body = {"id": 1, "name": "John Doe"}
        queues = ["queue1", "queue2"]

        publish(action, body, queues)

        mock_publish.assert_any_call(action, body, "queue1")
        mock_publish.assert_any_call(action, body, "queue2")
        self.assertEqual(mock_publish.call_count, len(queues))

    @patch("apexmq.producers.ApexMQProducerManager.publish")
    def test_publish_to_broadcast(self, mock_publish):
        """Test the `publish` function with broadcast to all queues."""
        action = "user.create"
        body = {"id": 1, "name": "John Doe"}
        queues = ["broadcast"]

        publish(action, body, queues)

        mock_publish.assert_any_call(action, body, "broadcast")
        self.assertEqual(mock_publish.call_count, 1)