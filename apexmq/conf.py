# conf.py
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def get_apexmq_settings() -> dict:
    """
    Fetches and validates the APEXMQ settings from the Django settings file.
    """
    user_settings = getattr(settings, "APEXMQ_SETTINGS", None)

    if not user_settings:
        raise ImproperlyConfigured(
            "APEXMQ_SETTINGS is not defined in your settings.py file."
        )

    # Validate the structure of the settings
    if "connections" not in user_settings or not isinstance(
        user_settings["connections"], dict
    ):
        raise ImproperlyConfigured(
            "APEXMQ_SETTINGS must contain a 'connections' dictionary."
        )

    if "queues" not in user_settings or not isinstance(user_settings["queues"], dict):
        raise ImproperlyConfigured(
            "APEXMQ_SETTINGS must contain a 'queues' dictionary."
        )

    return user_settings


def get_connection_params(name="default") -> dict:
    """
    Retrieve the apexmq connection parameters for the specified connection name.
    """
    settings = get_apexmq_settings()
    connections = settings.get("connections", {})

    if name not in connections:
        raise ImproperlyConfigured(
            f"apexmq connection '{name}' is not defined in APEXMQ_SETTINGS."
        )

    return connections[name]


def get_queue_settings(queue_name) -> dict:
    """
    Retrieve the queue settings for a given queue name.
    """
    settings = get_apexmq_settings()
    queues = settings.get("queues", {})

    if queue_name not in queues:
        raise ImproperlyConfigured(
            f"Queue '{queue_name}' is not defined in APEXMQ_SETTINGS."
        )

    return queues[queue_name]
