import logging
from functools import wraps
from typing import Callable, List, Literal
from django.db.models import Model
from django.db.models.signals import post_save, post_delete

from .conf import get_first_channel_name, info
from .connection import ApexMQChannelManager

logger = logging.getLogger(__name__)


def publish(
    action: str,
    body: dict,
    to: List[str] | Literal["broadcast"],
    channel_name=None,
):
    """
    Publishes a message to the specified queue(s) with the given action and body.

    Args:
        action (str): The action identifier for the message being published.
        body (dict): The data to be sent in the message.
        to (List[str] | Literal["broadcast"]): A list of queue names or "broadcast" where the message will be sent.
        channel_name (str, optional): The name of the channel through which the message
                                      will be published. Defaults to the first channel name
                                      configured in the system.
    Functionality:
        - The function gets the channel manager for the specified channel name.
        - The function publishes the message to the specified queue(s) using the
          `publish` method of the channel manager.
        - The function logs the success or failure of the publishing operation.
    Usage:
        publish("user.create", {"id": 1, "name": "John Doe"}, ["queue1", "queue2"])
        - This will send a message with the action "user.create" and the body
          {"id": 1, "name": "John Doe"} to the "queue1" and "queue2" queues.
        publish("user.create", {"id": 1, "name": "John Doe"}, "broadcast")
        - This will broadcast a message with the action "user.create" and the body
          {"id": 1, "name": "John Doe"} to all queues in the connection.
    """
    for publish_to in to:
        try:
            ApexMQChannelManager.publish(action, body, publish_to)
            info(f'"PUBLISHED - QUEUE: {publish_to} | ACTION: {action}"')
        except Exception as e:
            logger.error(f"Failed to publish message to {publish_to}: {e}")


def on_model_create(
    model,
    to: List[str] | Literal["broadcast"],
    fields: List[str],
    action: str = None,
    channel_name=None,
):
    """
    Registers a post-save signal handler to publish specific fields of a model
    instance when a new object is created.

    Args:
        model (Model): The Django model class for which the signal is registered.
        fields (List[str]): A list of field names to extract data from the model instance.
        action (str): The action identifier for the message being published (e.g., 'user.create').
        to (List[str]): A list of queue names where the message will be sent.
        channel_name (str, optional): The name of the channel through which the message
                                      will be published. Defaults to the first channel name
                                      configured in the system.

    Functionality:
        - The function connects a post-save signal to the specified model.
        - When a new instance of the model is created, the specified fields are extracted
          from the model instance.
        - A message is constructed with the extracted fields and sent to the specified
          queues via the `publish` function.

    Usage:
        on_model_create(User, ["id", "name"], "user.create", ["queue1", "queue2"])
        - This will send the `id` and `name` fields of a new `User` instance to
          "queue1" and "queue2" when a new user is created.
    """

    if not issubclass(model, Model):
        raise TypeError("The model argument must be a Django model class.")

    if not action:
        action = f"{model.__name__.lower()}.created"

    def on_create(sender, instance, created, **kwargs):
        if created:
            body = {field: getattr(instance, field) for field in fields}
            publish(action, body, to, channel_name)

    post_save.connect(on_create, sender=model)


def on_model_update(
    model,
    to: List[str] | Literal["broadcast"],
    fields: List[str] = None,
    action: str = None,
    channel_name=None,
):
    """
    Registers a post-save signal to trigger when a model instance is updated.

    Args:
        model (Model): The Django model class for which the signal is registered.
        to (List[str]): A list of queue names where the message will be sent.
        action (str, optional): The action identifier for the message being published. If not provided,
                                it should be returned by the decorated function.
        fields (List[str], optional): List of field names to extract data from the model instance.
                                      Required if no decorated function is used.
    Functionality:
        - The function connects a post-save signal to the specified model.
        - When an instance of the model is updated, the specified fields are extracted
          from the model instance.
        - A message is constructed with the extracted fields and sent to the specified
          queues via the `publish` function.
    Usage:
        1. As a decorator:
            @on_model_update(User, ["queue1", "queue2"])
            def on_user_update(instance):
                return ("custom.action", {"id": instance.id, "name": instance.name})

        2. As a function:
            on_model_update(User, ["queue1", "queue2"], "custom.action", ["id", "email", "username"])
    """
    if not issubclass(model, Model):
        raise TypeError("The model argument must be a Django model class.")

    if not action:
        action = f"{model.__name__.lower()}.updated"

    # Function decorator handling
    def decorator(func: Callable = None):
        @wraps(func)
        def on_update(sender, instance, created, **kwargs):
            if not created:  # Only trigger on updates (not creates)
                if func:
                    # If used as a decorator, get action and body from the decorated function
                    action_val, body = func(instance)
                else:
                    # If used directly, use the fields and action provided as arguments
                    if fields is None:
                        raise ValueError(
                            "Fields must be provided if not using a decorated function."
                        )
                    action_val = action
                    body = {field: getattr(instance, field) for field in fields}

                # Publish the action and body to the specified queues
                publish(action_val, body, to, channel_name)

        # Register the signal handler
        post_save.connect(on_update, sender=model)
        return func if func else on_update

    return decorator


def on_model_delete(
    model,
    to: List[str] | Literal["broadcast"],
    action: str = None,
    channel_name=None,
):
    """
    Registers a post-delete signal to trigger when a model instance is deleted.

    Args:
        model (Model): The Django model class for which the signal is registered.
        to (List[str]): A list of queue names where the message will be sent.
        action (str, optional): The action identifier for the message being published.
                                If not provided, defaults to '{model_name}.deleted'.
    Functionality:
        - The function connects a post-delete signal to the specified model.
        - When an instance of the model is deleted, a message is constructed with the
          instance's primary key and sent to the specified queues via the `publish` function
    Usage:
        1. on_model_delete(User, ["queue1", "queue2"], "custom.action")
    """

    if not issubclass(model, Model):
        raise TypeError("The model argument must be a Django model class.")

    if not action:
        action = f"{model.__name__.lower()}.deleted"

    def on_delete(sender, instance, **kwargs):
        publish(action, {"id": instance.pk}, to, channel_name)

    post_delete.connect(on_delete, sender=model)
