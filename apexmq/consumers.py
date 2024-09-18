import json
import logging
from typing import Dict
from django.core.exceptions import ImproperlyConfigured

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BaseConsumer:
    """
    Base class for message consumers.

    Attributes:
        lookup_prefix (str): The prefix used to identify which actions this consumer handles.
    """

    lookup_prefix = None

    def process_messege(self, action: str, data):
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
            else:
                msg = f"New action detected. Cannot find handling method for,\nAction: {action}"
                logger.warning(msg)
        else:
            msg = f"New action detected. Cannot find handling method for,\nAction: {action}"
            logger.warning(msg)


action_handlers = {}


def on_consume(action):
    """
    Decorator to register a function as a handler for a specific action.

    This decorator registers the decorated function in the global `action_handlers` dictionary
    with the specified action as the key. The function will be called with the provided data
    when the action is triggered.

    Args:
        action (str): The action type to register the handler for.

    Returns:
        function: The inner function that wraps the original function.

    Example:
        @on_consume("user.created")
        def user_create(data: dict):
            # Handle user.created action
            pass
    """

    def wrapper(f):
        action_handlers[action] = f

        def inner(data):
            f(data)

        return inner

    return wrapper
