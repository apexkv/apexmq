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

    if len(user_settings) == 0:
        raise ImproperlyConfigured(
            "Cannot find any host connections defined in APEXMQ_SETTINGS."
        )

    return user_settings


def get_connection_params(connection_name) -> dict:
    """
    Retrieve the apexmq connection parameters for the specified connection name.
    """
    settings = get_apexmq_settings()

    if connection_name not in settings:
        raise ImproperlyConfigured(
            f"'{connection_name}' is not defined in APEXMQ_SETTINGS."
        )

    connection_settings = settings[connection_name]

    if "user" not in connection_settings or "password" not in connection_settings:
        raise ImproperlyConfigured(
            f"Server logging data has not defined in '{connection_name}' configurations."
        )

    return connection_settings
