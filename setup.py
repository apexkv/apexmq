from setuptools import setup, find_packages

setup(
    name="apexmq",
    version="1.1.0",
    description="A developer-friendly library for integrating RabbitMQ with Django applications, enabling seamless message queuing, automated consumer registration, and simplified communication between microservices. ApexMQ is designed to streamline asynchronous messaging for Django projects, allowing developers to focus on business logic rather than managing connections and queues.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Kavindu Harshitha",
    author_email="kavindu@apexkv.com",
    license="Custom License",
    license_files=("LICENSE",),
    url="https://github.com/apexkv/apexmq",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: Django",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    install_requires=[
        "pika>=1.1.0",
        "django>=3.0.0",
    ],
    python_requires=">=3.6",
)
