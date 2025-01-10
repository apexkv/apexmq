import json
import pika
import time
import threading
from typing import Dict, List
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from django.core.exceptions import ImproperlyConfigured

from .conf import get_connection_params, info, error

class ApexMQQueueManager:
    pass

class ApexMQChannelManager:
    pass

class ApexMQConnection:
    def __init__(self):
        self.connect()

    def connect(self):
        pass


class ApexMQConnectionManager:
    connection: ApexMQConnection = None
    
    def __init__(self, connection_params: Dict):
        self.connection = ApexMQConnection()