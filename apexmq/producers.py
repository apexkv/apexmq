from typing import Callable, List
from django.db.models import Model
from django.db.models.signals import post_save, post_delete

from .managers import ApexMQProducerManager


def publish(
    action: str,
    body: dict,
    to: List[str]
):
    """
    Publishes a message to the specified queue(s) with the given action and body.

    Args:
        action (str): The action identifier for the message being published.
        body (dict): The data to be sent in the message.
        to List[str]: A list of queue names where the message will be sent.
    Functionality:
        - The function gets the channel manager for the specified channel name.
        - The function publishes the message to the specified queue(s) using the
          `publish` method of the channel manager.
        - The function logs the success or failure of the publishing operation.
    Usage:
        publish("user.create", {"id": 1, "name": "John Doe"}, ["queue1", "queue2"])
        - This will send a message with the action "user.create" and the body
          {"id": 1, "name": "John Doe"} to the "queue1" and "queue2" queues.
    """
    for publish_to in to:
        ApexMQProducerManager.publish(action, body, publish_to)


def on_model_action(model: Model, send_to: List[str]):
    """
    A decorator that listens for post_save and post_delete signals on a specified model.

    Args:
        model (Model): The model to listen for signals on.
        send_to (List[str]): A list of queue names to send the message to.

    Returns:
        Callable: The decorated function.

    Usage:
        @on_model_action(User, ["user"])
        def user_action(instance, created, updated, deleted):
            if created:
                return "user.create", {"id": instance.id, "name": instance.name}
            elif updated:
                return "user.update", {"id": instance.id, "name": instance.name}
            elif deleted:
                return "user.delete", {"id": instance.id}
    """
    def outer(func: Callable):
        def user_action(sender, instance, **kwargs):
            created = kwargs.get("created", False)
            updated = not created if "created" in kwargs else False
            deleted = kwargs.get("deleted", False)

            action, body = func(instance, created, updated, deleted)

            publish(action, body, send_to)

        post_save.connect(user_action, sender=model, dispatch_uid=f"{model.__name__}_post_save")
        post_delete.connect(user_action, sender=model, dispatch_uid=f"{model.__name__}_post_delete")

        return func
    return outer
