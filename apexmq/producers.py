import json
import logging
import pika
from typing import List

from .conf import get_first_channel_name
from .connection import ApexMQChannelManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def publish(
    action: str,
    body: dict,
    to: List[str],
    channel_name=get_first_channel_name(),
):
    channel_manager = ApexMQChannelManager.get_channel(channel_name)
    for publish_to in to:
        try:
            channel_manager.publish(action, body, publish_to)
        except Exception as e:
            logger.error(f"Failed to publish message to {publish_to}: {e}")
