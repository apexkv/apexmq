# ApexMQ

ApexMQ is a Django application designed to manage RabbitMQ connections and message consumers efficiently. It supports setting up RabbitMQ connections, creating channels and queues, and processing messages from RabbitMQ queues.

## Features

-   **RabbitMQ Connection Management**: Establish and manage RabbitMQ connections.
-   **Channel and Queue Management**: Create and manage channels and queues within RabbitMQ.
-   **Message Consumption**: Set up message consumers to handle messages from RabbitMQ queues.
-   **Autoreload Support**: Automatically reconfigure RabbitMQ connections when code changes are detected (in DEBUG mode).

## Installation

To use ApexMQ in your Django project, follow these steps:

1. **Add ApexMQ to your Django Project**

    Add `apexmq` to the `INSTALLED_APPS` list in your Django project's `settings.py`:

    ```python
    INSTALLED_APPS = [
        ...
        'apexmq',
        ...
    ]
    ```

2. **Configure RabbitMQ Settings**

    Define RabbitMQ settings in your Django `settings.py` file:

    ```python
    APEXMQ_SETTINGS = {
        'default': {
            'USER': 'your_username',
            'PASSWORD': 'your_password',
            'HOST': 'localhost',
            'PORT': 5672,
            'VIRTUAL_HOST': '/',
            'CHANNELS': {
                'channel_name': {
                    'QUEUES': {
                        'queue_name': {}
                    }
                }
            }
        }
    }
    ```

3. **Usages of Consumer**
   3.1 **Create a Consumer**
   To create a custom consumer, subclass `BaseConsumer` and define methods to handle specific actions:

    ```python
    # your_app/consumers.py

    from apexmq.consumers import BaseConsumer

    class MyCustomConsumer(BaseConsumer):
        lookup_prefix = "prefix"

        def created(self, action: str, data):
            """
            This method will get data for action type: prefix.created
            """
            print("You can handle ")

        def custom_action(self, data):
            """
            This method is for handle action type: prefix.custom.action
            """
            print(f"Handling 'some_action' with data: {data}")

    ```

    3.2 **Register your Consumer**
    Ensure that your consumer class is registered in the consumers dictionary within your Django app's `consumers.py` file:

    ```python
    # consumers.py

    consumers = {
        "my_queue": MyCustomConsumer,
    }

    ```
