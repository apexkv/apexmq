from setuptools import setup, find_packages

setup(
    name="apexmq",
    version="1.0.0",
    description="A developer-friendly library for integrating RabbitMQ with Django applications.",
    long_description="ApexMQ - An open-source, developer-friendly library for integrating RabbitMQ with Django applications, designed to simplify message-based microservice architectures. ApexMQ offers intuitive decorators, signal-based model integration, and seamless message handling for robust, scalable systems.",
    long_description_content_type="text/markdown",
    author="Kavindu Harshitha",
    author_email="kavindu@apexkv.com",
    license="Custom License",
    license_files=("LICENSE",),
    url="https://github.com/apexkv/apexmq",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(where="apexmq"),
    package_dir={"": "apexmq"},
    install_requires=[
        "pika>=1.1.0",
    ],
    python_requires=">=3.6",
)
