import django
from django.conf import settings

settings.configure(
    APEXMQ_SETTINGS = {
        "CONNECTION": {
            "USER": "user",
            "PASSWORD": "user",
            "HOST": "localhost",
            "RETRIES": 1,
        },
    }
)

django.setup()