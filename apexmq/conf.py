import logging
from typing import Dict
from pydantic import BaseModel
from django.utils import timezone
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


def get_project_name()->str:
    """
    Fetches the name of the Django project from the ROOT_URLCONF setting.
    """
    try:
        return settings.ROOT_URLCONF.split(".")[0].lower()
    except Exception as e:
        return "project"


def simplify_keys(d):
    """Recursively simplifies keys of a dictionary."""
    if isinstance(d, dict):
        return {key.lower(): simplify_keys(value) for key, value in d.items()}
    elif isinstance(d, list):
        return [simplify_keys(item) for item in d]
    else:
        return d


DEFAULT_QUEUE_NAME = get_project_name()
APP_NAME = "apexmq"


class ApexMQSettingsQueue(BaseModel):
    durable: bool = True
    exclusive: bool = False
    passive: bool = False
    auto_delete: bool = False


class ApexMQSettingsConnection(BaseModel):
    user: str
    password: str
    host: str = "localhost"
    port: int = 5672
    vhost: str = "/"
    queues: Dict[str, ApexMQSettingsQueue] = {
        DEFAULT_QUEUE_NAME: ApexMQSettingsQueue()
    }
    retries: int = 5
    heartbeat: int = 60


def get_apexmq_settings() -> dict:
    """
    Fetches and validates the APEXMQ settings from the Django settings file.

    Returns:
        dict: The APEXMQ settings.

    Raises:
        ImproperlyConfigured: If APEXMQ_SETTINGS is not defined or is empty in settings.
    """
    APEXMQ_SETTINGS = "APEXMQ_SETTINGS"
    # Retrieve the APEXMQ_SETTINGS from Django settings
    user_settings = getattr(settings, APEXMQ_SETTINGS, None)

    if not user_settings:
        raise ImproperlyConfigured(
            f"{APEXMQ_SETTINGS} is not defined in your settings.py file."
        )

    if len(user_settings) == 0:
        raise ImproperlyConfigured(
            f"Cannot find any host connections defined in {APEXMQ_SETTINGS}."
        )

    return simplify_keys(user_settings)


def get_connection_settings() -> ApexMQSettingsConnection:
    """
    Retrieve the apexmq connection settings from the APEXMQ_SETTINGS dictionary.

    Returns:
        dict: The connection settings.

    Raises:
        ImproperlyConfigured: If "CONNECTION" not in APEXMQ_SETTINGS.
    """
    CONNECTION = "connection"
    settings = get_apexmq_settings()

    if CONNECTION not in settings:
        raise ImproperlyConfigured(f"{CONNECTION} is not defined in APEXMQ_SETTINGS.")

    connection = ApexMQSettingsConnection(**settings[CONNECTION])

    return connection


class Logger:
    logger = logging.getLogger(__name__)

    def log(self, msg):
        timestamp = timezone.now()
        details = f"[{timestamp.day:02d}/{timestamp.month:02d}/{timestamp.year} {timestamp.hour:02d}:{timestamp.minute:02d}:{timestamp.second:02d}] {msg}"
        return details
    
    @classmethod
    def info(cls, msg):
        details = cls().log(msg)
        cls.logger.info(details)
        print(details)
    
    @classmethod
    def warning(cls, msg):
        details = cls().log(msg)
        cls.logger.warning(details)

    @classmethod
    def error(cls, msg):
        details = cls().log(msg)
        cls.logger.error(details)

    @classmethod
    def debug(cls, msg):
        details = cls().log(msg)
        cls.logger.debug(details)
        print(details)