import json
from unittest import TestCase
from unittest.mock import patch
from apexmq.consumers import BaseConsumer
from django.core.exceptions import ImproperlyConfigured


class TestBaseConsumer(TestCase):
    def test_init_without_lookup_prefix(self):
        class ConsumerWithoutPrefix(BaseConsumer):
            pass

        with self.assertRaises(ImproperlyConfigured) as context:
            ConsumerWithoutPrefix(action="test.action", data="{}")
        self.assertEqual(str(context.exception), "Need to configure lookup_prefix.")

    def test_action_with_matching_prefix_and_existing_method(self):
        class ValidConsumer(BaseConsumer):
            lookup_prefix = "test"

            def action_subaction(self, data):
                self.called_data = data

        consumer = ValidConsumer(action="test.action.subaction", data=json.dumps({"key": "value"}))
        self.assertTrue(consumer.method_found)
        self.assertEqual(consumer.called_data, {"key": "value"})

    def test_action_with_matching_prefix_and_missing_method(self):
        class MissingMethodConsumer(BaseConsumer):
            lookup_prefix = "test"

        with patch("apexmq.consumers.Logger.warning") as mock_logger:
            consumer = MissingMethodConsumer(action="test.action.missing", data="{}")
            self.assertFalse(consumer.method_found)
            mock_logger.assert_called_with(
                "New action detected. Cannot find handling method in MissingMethodConsumer for Action: test.action.missing"
            )

    def test_action_with_non_matching_prefix(self):
        class NonMatchingPrefixConsumer(BaseConsumer):
            lookup_prefix = "other"

        with patch("apexmq.consumers.Logger.warning") as mock_logger:
            consumer = NonMatchingPrefixConsumer(action="test.action", data="{}")
            self.assertFalse(consumer.method_found)
            mock_logger.assert_called_with(
                "New action detected. Cannot find handling method in NonMatchingPrefixConsumer for Action: test.action"
            )
