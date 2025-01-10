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
