from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import os


def publish_message(kafka_producer, topic, message, headers):
    """Publish a single message to Kafka."""
    try:
        kafka_producer.publish(topic, message, headers)
        logging.info(f"Successfully published message to topic {topic}: {message}")
    except Exception as e:
        logging.error(f"Error publishing message to topic {topic}: {e}")
        raise e


def process_file(file_path, kafka_producer, processed_dir, error_dir, num_publish_threads=20):
    """Process a single JSON file and publish messages in parallel."""
    try:
        logging.info(f"Processing file: {file_path}")

        # Determine Kafka topic based on file type
        if "transaction" in os.path.basename(file_path):
            topic = "flight-transaction-update"
        elif "location" in os.path.basename(file_path):
            topic = "flight-location-update"
        else:
            raise ValueError(f"Unknown file type: {file_path}")

        # Prepare thread pool for parallel publishing
        with ThreadPoolExecutor(max_workers=num_publish_threads) as executor:
            with open(file_path, "r") as file:
                for line_number, line in enumerate(file, start=1):
                    data = line.strip()
                    headers = {
                        "eventName": "file.processed",
                        "eventTimestamp": datetime.utcnow().isoformat(),
                    }

                    # Submit each message to the thread pool for parallel publishing
                    executor.submit(publish_message, kafka_producer, topic, data, headers)

        # Move file to processed directory after successful processing
        # os.rename(file_path, os.path.join(processed_dir, os.path.basename(file_path)))
        logging.info(f"File {file_path} processed successfully.")

    except Exception as e:
        # Move file to error directory in case of failure
        # os.rename(file_path, os.path.join(error_dir, os.path.basename(file_path)))
        logging.error(f"Error processing file {file_path}: {e}")
