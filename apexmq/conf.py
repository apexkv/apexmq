import importlib
import inspect
import logging
from django.utils import timezone
from django.conf import settings
from django.apps import apps
from django.core.exceptions import ImproperlyConfigured

from .consumers import BaseConsumer


def get_apexmq_settings() -> dict:
    """
    Fetches and validates the APEXMQ settings from the Django settings file.

    Returns:
        dict: The APEXMQ settings.

    Raises:
        ImproperlyConfigured: If APEXMQ_SETTINGS is not defined or is empty in settings.
    """
    # Retrieve the APEXMQ_SETTINGS from Django settings
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


def get_connection_settings() -> dict:
    """
    Retrieve the apexmq connection settings from the APEXMQ_SETTINGS dictionary.

    Returns:
        dict: The connection settings.

    Raises:
        ImproperlyConfigured: If "CONNECTION" not in APEXMQ_SETTINGS.
    """
    KEY = "CONNECTION"
    # Fetch APEXMQ settings
    settings = get_apexmq_settings()

    if KEY not in settings:
        raise ImproperlyConfigured(f"{KEY} is not defined in APEXMQ_SETTINGS.")

    return settings[KEY]


def get_connection_params(connection_name) -> dict:
    """
    Retrieve the apexmq connection parameters for the specified connection name.

    Args:
        connection_name (str): The name of the connection.

    Returns:
        dict: The connection parameters for the specified connection name.

    Raises:
        ImproperlyConfigured: If the connection name is not found or required fields are missing.
    """
    # Fetch APEXMQ settings
    settings = get_connection_settings()

    if connection_name not in settings:
        raise ImproperlyConfigured(
            f"'{connection_name}' is not defined in APEXMQ_SETTINGS."
        )

    connection_settings = settings[connection_name]

    # Ensure that required fields are present in the connection settings
    if "USER" not in connection_settings or "PASSWORD" not in connection_settings:
        raise ImproperlyConfigured(
            f"Server logging data has not defined in '{connection_name}' configurations."
        )

    return connection_settings


def get_consumers_from_apps():
    """
    Retrieves all consumer classes that are subclasses of BaseConsumer from the consumers.py files of installed apps.

    Returns:
        list: A list of the consumer classes.

    Notes:
        - Iterates over all installed apps and attempts to import their consumers.py module.
        - If the module contains classes that are subclasses of BaseConsumer, they are added to the result.
    """
    consumers_dict = []
    FILE_NAME = "consumers"

    # Iterate over all installed apps
    for app_config in apps.get_app_configs():
        if app_config.name != "apexmq":
            try:
                # Construct the path to the consumer.py file in the app
                module_path = f"{app_config.name}.{FILE_NAME}"

                # Dynamically import the consumer.py module
                module = importlib.import_module(module_path)

                class_list = inspect.getmembers(module, inspect.isclass)

                for name, classobject in class_list:
                    if issubclass(classobject, BaseConsumer):
                        consumers_dict.append(classobject)

            except ModuleNotFoundError:
                # Skip if the app doesn't have a consumers.py file
                continue
            except Exception as e:
                continue

    return consumers_dict


logger = logging.getLogger(__name__)


def info(msg):
    timestamp = timezone.now()
    details = f"[{timestamp.day:02d}/{timestamp.month:02d}/{timestamp.year} {timestamp.hour:02d}:{timestamp.minute:02d}:{timestamp.second:02d}] {msg}"
    logger.info(details)
    print(details)


def warning(msg):
    timestamp = timezone.now()
    details = f"[{timestamp.day:02d}/{timestamp.month:02d}/{timestamp.year} {timestamp.hour:02d}:{timestamp.minute:02d}:{timestamp.second:02d}] {msg}"
    logger.warning(details)


def error(msg):
    timestamp = timezone.now()
    details = f"[{timestamp.day:02d}/{timestamp.month:02d}/{timestamp.year} {timestamp.hour:02d}:{timestamp.minute:02d}:{timestamp.second:02d}] {msg}"
    logger.error(details)
