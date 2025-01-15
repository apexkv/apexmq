import sys, atexit, threading
from django.apps import AppConfig
from django.utils.autoreload import autoreload_started

from .conf import Logger
from .connection import ApexMQManager


threads = []
threads_lock = threading.Lock()

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

        atexit.register(self.cleanup_threads)

    def watch_for_changes(self):
        """
        Connects the `setup_rabbitmq` method to the `autoreload_started` signal.
        This method will be called whenever Django detects a code change.
        """
        autoreload_started.connect(self.setup_rabbitmq)

    def setup_rabbitmq(self, **kwargs):
        self.manager = ApexMQManager()
        manager_thread = threading.Thread(target=self.manager.ready, name="ManagerThread", daemon=True)
        manager_thread.start()
        global threads
        with threads_lock:
            threads.append(manager_thread)

    def cleanup_threads(self):
        """
        Ensures all RabbitMQ threads terminate gracefully when the application shuts down.
        """

        with threads_lock:
            for thread in threads:
                thread.join(timeout=5)

        Logger.info("All RabbitMQ threads shut down.")

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