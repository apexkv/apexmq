import importlib
from django.conf import settings
from django.apps import apps
from django.core.exceptions import ImproperlyConfigured


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
    settings = get_apexmq_settings()

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
    Retrieves all consumer classes from the consumers.py files of installed apps.

    Returns:
        dict: A dictionary with consumer names as keys and consumer classes as values.

    Notes:
        - Iterates over all installed apps and attempts to import their consumers.py module.
        - If the module contains a 'consumers' dictionary, it is added to the returned dictionary.
    """
    consumers_dict = {}
    FILE_NAME = "consumers"
    DICTIONARY_NAME = "consumers"

    # Iterate over all installed apps
    for app_config in apps.get_app_configs():
        try:
            # Construct the path to the consumer.py file in the app
            module_path = f"{app_config.name}.{FILE_NAME}"

            # Dynamically import the consumer.py module
            module = importlib.import_module(module_path)

            # Check if the module has a dictionary named 'consumers'
            if hasattr(module, DICTIONARY_NAME) and isinstance(
                getattr(module, DICTIONARY_NAME), dict
            ):
                consumers_dict.update(getattr(module, DICTIONARY_NAME))

        except ModuleNotFoundError:
            # Skip if the app doesn't have a consumers.py file
            continue
        except Exception as e:
            print(f"Error while loading consumers from {app_config.name}: {e}")

    return consumers_dict
