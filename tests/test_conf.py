from unittest import TestCase
from unittest.mock import patch, MagicMock
from django.core.exceptions import ImproperlyConfigured
from apexmq.conf import get_apexmq_settings, get_connection_settings, get_project_name, ApexMQSettingsConnection


class TestConf(TestCase):
    @patch("apexmq.conf.settings")
    def test_get_apexmq_settings(self, mock_settings):
        expected_result = {
            "connection": {
                "user": "test",
                "password": "test"
            }
        }

        mock_settings.APEXMQ_SETTINGS = expected_result

        self.assertEqual(get_apexmq_settings(), expected_result)

    @patch("apexmq.conf.settings")
    def test_get_apexmq_settings_none(self, mock_settings):
        mock_settings.APEXMQ_SETTINGS = None
        with self.assertRaises(ImproperlyConfigured) as context:
            get_apexmq_settings()

        self.assertEqual(str(context.exception), "APEXMQ_SETTINGS is not defined in your settings.py file.")

    @patch("apexmq.conf.settings")
    def test_get_connection_settings(self, mock_settings):
        expected_settings = {
            "user": "test",
            "password": "test"
        }

        mock_settings.APEXMQ_SETTINGS = {
            "connection": expected_settings
        }
        
        expected_result = ApexMQSettingsConnection(**expected_settings)

        self.assertEqual(get_connection_settings(), expected_result)

    @patch("apexmq.conf.settings")
    def test_get_connection_settings_missing_connection(self, mock_settings):
        mock_settings.APEXMQ_SETTINGS = {}
        with self.assertRaises(ImproperlyConfigured) as context:
            get_connection_settings()

        self.assertEqual(str(context.exception), "APEXMQ_SETTINGS is not defined in your settings.py file.")

    @patch("apexmq.conf.settings")
    def test_get_project_name(self, mock_settings):
        mock_settings.ROOT_URLCONF = "test.urls"
        self.assertEqual(get_project_name(), "test")

    @patch("apexmq.conf.settings")
    def test_get_project_name_missing_root_urlconf(self, mock_settings):
        mock_settings.ROOT_URLCONF = None

        expect_result = "project"

        get_project_name()

        self.assertEqual(get_project_name(), expect_result)