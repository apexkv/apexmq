# ApexMQ

**ApexMQ** is a Django application that streamlines the integration of RabbitMQ with your Django projects. By simplifying the management of RabbitMQ connections, channels, and queues, ApexMQ makes it easy to build scalable, message-driven architectures. Whether you are handling user actions, notifications, or inter-service communication, ApexMQ ensures your system can communicate asynchronously and efficiently.

## Features

-   **RabbitMQ Connection Management**: Seamlessly establish and manage RabbitMQ connections with automatic retries.
-   **Channel and Queue Management**: Create, configure, and manage RabbitMQ channels and queues with ease.
-   **Autoreload in Development**: Automatically reload RabbitMQ connections when code changes are detected (in DEBUG mode).
-   **Class-Based Consumers**: Automatically register and manage consumer classes for message handling.
-   **Message Production**: Publish messages to one or more RabbitMQ queues with minimal code.
-   **Advanced Queue Configuration**: Full control over queue options, including durability, exclusivity, and auto-acknowledgment.

## Installation

To integrate **ApexMQ** into your Django project, follow the steps below:

### 1. Install ApexMQ

Install the library using pip:

```
pip install apexmq
```

### 2. Add ApexMQ to Django Project

In your Django project's `settings.py`, add `apexmq` to the `INSTALLED_APPS` list:

```python
INSTALLED_APPS = [
    ...
    'apexmq',
    ...
]
```

### 3. Configure ApexMQ Settings

Define the necessary RabbitMQ settings in your `settings.py` file:

```python
APEXMQ_SETTINGS = {
    "CONNECTIONS": {
        "default": {
            "USER": "your_username",
            "PASSWORD": "your_password",
            "HOST": "localhost",
            "PORT": 5672,  # optional
            "VHOST": "/",   # optional
            "RETRIES": 5,   # optional
            "HEARTBEAT": 60, # optional
            "QUEUES": {
                "queue_name": {
                    "DURABLE": True,  # optional
                    "EXCLUSIVE": False,  # optional
                    "PASSIVE": False,  # optional
                    "AUTO_DELETE": False,  # optional
                }
            }
        }
    }
}
```

The configuration allows you to specify multiple RabbitMQ connections and fine-tune queue properties like durability, auto-acknowledgment, and exclusivity.

---

## Consumer Usage

### 1. Create a Consumer

To create a custom consumer, subclass `BaseConsumer` and implement methods to handle different message actions. ApexMQ will automatically register consumers in your `your_app/consumers.py`.

Example consumer:

```python
# your_app/consumers.py

from apexmq.consumers import BaseConsumer

class MyCustomConsumer(BaseConsumer):
    lookup_prefix = "prefix"

    def created(self, data):
        """
        This method will handle data for action type: prefix.created
        """
        print("Handling 'created' action:", data)

    def updated(self, data):
        """
        This method will handle data for action type: prefix.updated
        """
        print("Handling 'updated' action:", data)
```

**Note:** There's no need to manually register your consumer class. ApexMQ automatically discovers and registers all consumers defined in `your_app/consumers.py`.

### 2. Handle Multiple Actions in One Consumer

You can define multiple methods in your consumer class to handle different action types. The method names should match the action type suffix (e.g., `user.created`, `user.updated`).

-   `prefix.created` → `created()`
-   `prefix.updated` → `updated()`

### 3. Error Handling

If you need to handle errors during consumption, you can raise exceptions within the consumer methods, and ApexMQ will retry or log the error depending on the configuration. For more robust error handling, you can integrate with Django's logging system or custom retry logic.

---

## Producer Usage

### 1. Publish Messages to Multiple Queues

ApexMQ allows you to easily publish messages to multiple queues using the `publish` function.

```python
from apexmq.producers import publish

# Send a message to multiple queues
publish(
    action="user.created",
    data={"id": 1, "username": "janedoe", "email": "jan@example.com"},
    to=["products", "inventory", "notifications"]
)
```

-   **action**: The action type associated with the message (e.g., `user.created`).
-   **data**: The message content, typically in dictionary format.
-   **to**: A list of queue names where the message will be sent.

### 2. Model-Specific Action Handling

You can also use the `on_model_action` decorator to send messages automatically when a model is created, updated, or deleted. This allows for a more dynamic approach to publishing messages in response to changes in your Django models.

Example:

```python
from apexmq.producers import on_model_action

@on_model_action(model=User, send_to=["user_updates"])
def user_action(instance, created, updated, deleted):
    if created:
        return "user.created", {"id": instance.id, "name": instance.name}
    elif updated:
        return "user.updated", {"id": instance.id, "name": instance.name}
    elif deleted:
        return "user.deleted", {"id": instance.id}
```

**Note:** The `on_model_action` decorator returns a tuple with the action type and the message data.

### 3. Advanced Queue Binding (Optional)

For advanced users, ApexMQ allows you to bind or unbind queues dynamically. This feature is useful when handling complex routing scenarios or when different consumers need to consume messages from different queues based on the action type.

---

## Autoreload in Development

In **DEBUG** mode, ApexMQ will automatically reload RabbitMQ connections whenever you make changes to your codebase. This is useful during development but should be disabled in production environments to avoid unnecessary disruptions.

---

## Testing and Debugging Consumers and Producers

To ensure reliability, it’s essential to test both consumers and producers. For consumers, use unit tests that simulate incoming messages and assert the expected behavior. For producers, you can mock the `publish` function to test that the correct messages are sent to the correct queues.

---

## Best Practices

1. **Idempotent Consumers**: Ensure your consumer methods are idempotent. This means that if a message is processed more than once, it won’t result in unintended side effects (e.g., duplicate records).
2. **Message Acknowledgment**: Always acknowledge messages after processing to prevent them from being re-queued unnecessarily. If a message processing fails, handle it by either retrying or sending it to a dead-letter queue.
3. **Connection Pooling**: For high-load applications, consider implementing connection pooling to manage RabbitMQ connections efficiently.
4. **Environment Variables**: Store sensitive information such as RabbitMQ credentials in environment variables rather than hardcoding them in your `settings.py`.

---

## Contribution

We welcome contributions! To contribute to **ApexMQ**, please follow these steps:

1. Fork the repository and clone it locally.
2. Install the dependencies by running `pip install -r requirements.txt`.
3. Create a new branch for your changes.
4. Write tests for new features or bug fixes.
5. Submit a pull request with a description of your changes.

---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

---

### Summary

With **ApexMQ**, you can quickly integrate RabbitMQ with Django and manage message queues with minimal boilerplate. By leveraging class-based consumers, flexible message producers, and automatic registration of consumers, you can focus more on your application’s business logic and leave the messaging infrastructure to ApexMQ. Whether you’re handling real-time notifications, background jobs, or inter-service communication, **ApexMQ** provides a robust and scalable solution for your Django project.
