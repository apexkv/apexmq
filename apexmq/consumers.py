import json, inspect, importlib
from django.apps import apps
from django.core.exceptions import ImproperlyConfigured
from .conf import Logger, APP_NAME


def get_consumers_from_apps():
    """
    Retrieves all consumer classes that are subclasses of BaseConsumer from the consumers.py files of installed apps.

    Returns:
        list: A list of the consumer classes.

    Notes:
        - Iterates over all installed apps and attempts to import their consumers.py module.
        - If the module contains classes that are subclasses of BaseConsumer, they are added to the result.
    """
    consumers = {}
    FILE_NAME = "consumers"

    # Iterate over all installed apps
    for app_config in apps.get_app_configs():
        if app_config.name != APP_NAME:
            try:
                # Construct the path to the consumer.py file in the app
                module_path = f"{app_config.name}.{FILE_NAME}"

                # Dynamically import the consumer.py module
                module = importlib.import_module(module_path)

                class_list = inspect.getmembers(module, inspect.isclass)

                for name, classobject in class_list:
                    if issubclass(classobject, BaseConsumer) and classobject != BaseConsumer:
                        consumers[classobject.lookup_prefix] = classobject

            except ModuleNotFoundError:
                # Skip if the app doesn't have a consumers.py file
                continue
            except Exception as e:
                continue
    
    return consumers



class BaseConsumer:
    """
    Base class for message consumers.

    Attributes:
        lookup_prefix (str): The prefix used to identify which actions this consumer handles.
    """

    lookup_prefix = None

    def __init__(self, action: str, data):
        """
        Processes a message based on the action type and data.

        Args:
            action (str): The action type as a string, typically in the format "prefix.action.subaction".
            data (str): The message data as a JSON-encoded string.

        Raises:
            ImproperlyConfigured: If `lookup_prefix` is not configured.

        Notes:
            - The `action` string is split into parts using "." as a delimiter.
            - The `lookup_prefix` is compared with the first part of the `action`.
            - If they match, the method corresponding to the remaining parts of the `action` is called.
            - If no matching method is found, a message is printed.
        """
        self.method_found = False

        # Ensure the consumer has a configured lookup prefix
        if not self.lookup_prefix:
            raise ImproperlyConfigured("Need to configure lookup_prefix.")

        action_types = action.split(".")

        # Check if the prefix matches
        if self.lookup_prefix == action_types[0]:
            # Create the method name from the remaining parts of the action
            method_name = "_".join(action_types[1:])
            method = getattr(self, method_name, None)

            # Call the method if it exists and is callable
            if callable(method):
                method(json.loads(data))
                self.method_found = True
            else:
                msg = f"New action detected. Cannot find handling method in {self.__class__.__name__} for Action: {action}"
                Logger.warning(msg)
        else:
            msg = f"New action detected. Cannot find handling method in {self.__class__.__name__} for Action: {action}"
            Logger.warning(msg)
        
        