import requests
from confluent_kafka import Producer
import json
import logging
import yaml
import os


class KafkaProducerWrapper:
    def __init__(self, kafka_config):
        """Initialize the Kafka producer with configuration from the config file."""
        self.producer = Producer({"bootstrap.servers": kafka_config["bootstrap_servers"]})

    def publish(self, topic, message, headers):
        """Publish a message to a Kafka topic."""
        try:
            # Convert the message to a JSON string
            message_json = json.loads(message) if isinstance(message, str) else message
            message_str = json.dumps(message_json)

            # Publish the message
            self.producer.produce(
                topic,
                value=message_str.encode("utf-8"),
                headers=[(k, v.encode("utf-8")) for k, v in headers.items()]
            )
            self.producer.flush()
            print(f"Published message to topic: {topic}")
            logging.info(f"Message published to topic {topic}.")
        except Exception as e:
            logging.error(f"Failed to publish message to topic {topic}: {e}")


def initialize_logging(log_config):
    """Initialize logging based on config."""
    log_file_path = log_config.get("log_file_path", "app.log")
    logging.basicConfig(
        filename=log_file_path,
        level=getattr(logging, log_config["level"], logging.INFO),
        format=log_config["format"]
    )
    logging.info("Logging initialized.")


def load_config(config_path="config.yaml"):
    """Load the configuration from the YAML file."""
    with open(config_path, "r") as config_file:
        return yaml.safe_load(config_file)



