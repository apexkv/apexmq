import sys
from django.apps import AppConfig
from django.utils.autoreload import autoreload_started

from .managers import ApexMQManager


class ApexMQConfig(AppConfig):
    name = "apexmq"
    label = "ApexMQ"

    def ready(self):
        """
        Called when Django starts. If in DEBUG mode, sets up the autoreload
        listener to monitor code changes and reconfigure RabbitMQ connections.
        """
        from django.conf import settings

        if self.is_management_command_to_skip():
            return

        if settings.DEBUG:
            self.watch_for_changes()
        else:
            self.setup_rabbitmq()

    def watch_for_changes(self):
        """
        Connects the `setup_rabbitmq` method to the `autoreload_started` signal.
        This method will be called whenever Django detects a code change.
        """
        autoreload_started.connect(self.setup_rabbitmq)

    def setup_rabbitmq(self, **kwargs):
        self.manager = ApexMQManager()
        self.manager.ready()

    @staticmethod
    def is_management_command_to_skip():
        """
        Determines if the current management command should skip RabbitMQ setup.
        Returns True for commands like `makemigrations`, `migrate`, `collectstatic`, etc.
        """
        management_commands_to_skip = [
            "makemigrations",
            "migrate",
            "collectstatic",
            "test",
            "shell",
            "createsuperuser",
        ]
        return len(sys.argv) > 1 and sys.argv[1] in management_commands_to_skip