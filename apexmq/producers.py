import json
import pika
from typing import List
from .connection import ApexMQChannelManager


def producer(
    action: str,
    body: dict,
    to: List[str],
    channel_name=ApexMQChannelManager.get_first_channel_name(),
):
    channel = ApexMQChannelManager.get_channel(channel_name).channel
    properties = pika.BasicProperties(action)
    for publish_to in to:
        try:
            # Ensure the queue exists (even if declared by other microservices)
            channel.queue_declare(queue=publish_to, durable=True)

            # Publish the message to the queue
            channel.basic_publish(
                exchange="",
                routing_key=publish_to,  # Send directly to the queue
                body=json.dumps(body),
                properties=properties,
            )
        except Exception as e:
            print(f"Failed to publish message to {publish_to}: {e}")
