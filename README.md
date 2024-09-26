# ApexMQ

ApexMQ is a Django application designed to manage RabbitMQ connections and message consumers efficiently. It supports setting up RabbitMQ connections, creating channels and queues, and processing messages from RabbitMQ queues.

## Features

-   **RabbitMQ Connection Management**: Establish and manage RabbitMQ connections.
-   **Channel and Queue Management**: Create and manage channels and queues within RabbitMQ.
-   **Autoreload Support**: Automatically reconfigure RabbitMQ connections when code changes are detected (in DEBUG mode).
-   **Class-Based Consumers:** Automatically register consumer classes in `consumers.py.`
-   **Message Production:** Easily send messages to multiple queues using a producer.

## Installation

To use ApexMQ in your Django project, follow these steps:

1. **Install ApexMQ Library**

```
pip install apexmq
```

2. **Add ApexMQ to your Django Project**

    Add `apexmq` to the `INSTALLED_APPS` list in your Django project's `settings.py`:

    ```python
    INSTALLED_APPS = [
        ...
        'apexmq',
        ...
    ]
    ```

3. **Configure ApexMQ Settings**

    Define ApexMQ settings in your Django `settings.py` file:

    ```python
    APEXMQ_SETTINGS = {
        "default": {
            "USER": "your_username",
            "PASSWORD": "your_password",
            "HOST": "localhost",
            "PORT": 5672, # optional
            "VIRTUAL_HOST": "/", # optional
            "CONNECTION_TIMEOUT": 10, # optional
            "HEARTBEAT": 60, # optional
            "MAX_RETRIES": 5, # optional
            "RETRY_DELAY": 5, # optional
            "CHANNELS": {
                "channel_name": {
                    "QUEUES": {
                        "queue_name": {
                            "DURABLE": True, # optional
                            "EXCLUSIVE": False, # optional
                            "PASSIVE": False, # optional
                            "AUTO_DELETE": False, # optional
                            "AUTO_ACK": True, # optional
                        }
                    }
                }
            },
        }
    }
    ```

4. **Usages of Consumer**
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

    > **Note:** There's no need to manually register your consumer class. ApexMQ automatically discovers and registers all consumers defined in your_app/consumers.py.

    3.2 **Handle Multiple Actions in One Consumer**
    You can define multiple methods in your consumer class to handle different actions. The method name should match the suffix of the action type you're handling. For example:

    - If the action type is `user.created`, ApexMQ will call the `created()` method.
    - If the action type is `user.updated`, it will call the `updated()` method.

        3.2 **Consume Decorator**
        You can consume messages using the on_consume decorator. This decorator registers a function as a handler for a specific action.

    ```python
    from apexmq.consumers import on_consume

    @on_consume("user.created")
    def user_create(data: dict):
        # Handle user.created action
        print(f"User created: {data}")
    ```

---

5. **Usages of Producers**
   ApexMQ provides a simple way to publish messages to multiple RabbitMQ queues. The producer can be imported from apexmq.producers and allows you to send messages to multiple queues in one call.
   5.1. **Using the Producer**
   To send messages, use the producer() function:

```python
from apexmq.producers import publish, on_model_create, on_model_update, on_model_delete

# Send a message to multiple queues
publish(
    action="user.created",
    data={"id": 1, "username": "janedoe", "email": "jan@example.com"},
    to=["products", "inventory", "notifications"]
)

# this function will send id and name fields to "queue1", "queue2" queues action as "user.create".
# if you didnt provide action action will autocreate as modelname.create
on_model_create(User, ["queue1", "queue2"], ["id", "name"], "user.create")

# this function will send id and email fields to "queue1", "queue2" queues action as "user.update".
# if you didnt provide action action will autocreate as modelname.update
on_model_update(User, ["queue1", "queue2"], ["id", "email"], "user.update")

# now in this developer have more flexibility when model update. need to return tuple (action:str, data:dict)
@on_model_update(User, ["queue1", "queue2"])
def on_user_update(instance):
    return (
        "user.customaction",
        {
            "id": instance.id,
            "name": instance.name,
            "email": instance.email,
            "custom_field" custom_value
        }
    )

# this function will send id field to "queue1", "queue2" queues action as "user.deleted".
# if you didnt provide action action will autocreate as modelname.deleted
on_model_delete(User, ["queue1", "queue2"], "user.delete")

```

-   **action:** The action type associated with the message (e.g., `user.created`).
-   **data:** The message body, typically a dictionary.
-   **to:** A list of queue names to send the message to.

5.2. **Example Use Case**

For example, when a user is created in your system, you can send a message to the products, inventory, and notifications queues simultaneously, informing each of these services about the new user.

#### Summary

With ApexMQ, you can efficiently manage RabbitMQ connections and messages in your Django project:

-   Class-based consumers are automatically registered when defined in your_app/consumers.py.
-   The producer provides an easy-to-use interface for sending messages to multiple queues with a single function call.

ApexMQ simplifies RabbitMQ integration in Django and allows you to focus more on handling business logic instead of managing connections and consumers manually.

## Contributing

We welcome contributions! Please follow the steps below to get started:

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
