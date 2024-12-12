import logging
from concurrent.futures import ThreadPoolExecutor
import json
import yaml
import os
from datetime import datetime
from utils.kafka_utils import KafkaProducerWrapper
from utils.schema_validator import SchemaValidator

# Load configuration
current_directory = os.getcwd()

config_file_path = os.path.join(current_directory, "config.yaml")
with open(config_file_path, "r") as config_file:
    CONFIG = yaml.safe_load(config_file)

# File paths and settings from config
TRANSACTION_SCHEMA_PATH = os.path.join(current_directory, CONFIG["app"]["transaction_schema"])
LOCATION_SCHEMA_PATH = os.path.join(current_directory, CONFIG["app"]["location_schema"])
TRANSACTION_FILE_PATH = os.path.join(current_directory, CONFIG["app"]["transaction_file"])
LOCATION_FILE_PATH = os.path.join(current_directory, CONFIG["app"]["location_file"])
LOG_FILE_PATH = os.path.join(current_directory, CONFIG["app"]["log_file"])
CHUNK_SIZE = CONFIG["app"]["chunk_size"]
THREAD_COUNT = CONFIG["app"]["thread_count"]

# Kafka settings
KAFKA_CONFIG = CONFIG["kafka"]
TRANSACTION_TOPIC = KAFKA_CONFIG["topic_transaction"]
LOCATION_TOPIC = KAFKA_CONFIG["topic_location"]
DLQ_TOPIC = KAFKA_CONFIG["topic_dlq"]

# Schema and topic mappings
schema_dict = {TRANSACTION_FILE_PATH: TRANSACTION_SCHEMA_PATH, LOCATION_FILE_PATH: LOCATION_SCHEMA_PATH}
topic_dict = {TRANSACTION_FILE_PATH: TRANSACTION_TOPIC, LOCATION_FILE_PATH: LOCATION_TOPIC}

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Reconfigure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def process_chunk(chunk, schema_val_obj, kafka_producer_obj, topic):
    """Processes a single chunk of lines."""
    logging.info(f"Processing chunk with {len(chunk)} lines for topic '{topic}'.")
    futures = []
    with ThreadPoolExecutor(max_workers=5) as executor:  # Thread pool for chunk processing
        for line in chunk:
            if line:  # Ensure the line is not empty
                try:
                    # Validate JSON data
                    logging.debug(f"Validating JSON line: {line}")
                    isValid = schema_val_obj.validate_json(json.loads(line))
                    if not isValid:
                        raise ValueError("Invalid JSON schema")
                    logging.debug(f"Line validated successfully.")

                    # Kafka message headers
                    headers = {
                        "eventName": "file.processed",
                        "eventTimestamp": datetime.utcnow().isoformat(),
                    }

                    # Submit to Kafka producer
                    logging.info(f"Submitting line to Kafka topic '{topic}'.")
                    futures.append(executor.submit(kafka_producer_obj.publish, topic, line.strip(), headers))
                except Exception as e:
                    logging.error(f"Error processing line: {e}")
                    # Send failed messages to Dead Letter Queue
                    try:
                        dlq_headers = {
                            "error": str(e),
                            "failedTopic": topic,
                            "eventTimestamp": datetime.utcnow().isoformat(),
                        }
                        kafka_producer_obj.publish(DLQ_TOPIC, line.strip(), dlq_headers)
                        logging.info(f"Message sent to DLQ topic '{DLQ_TOPIC}'.")
                    except Exception as dlq_error:
                        logging.error(f"Failed to send message to DLQ: {dlq_error}")

        # Wait for all chunk futures to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in Kafka publishing thread: {e}")


def process_file(filename, schema_path, topic, kafka_producer_obj):
    """Processes a single file in chunks."""
    logging.info(f"Starting to process file: {filename}")
    schema_val_obj = SchemaValidator(schema_path)  # Load schema for the file
    total_lines_processed = 0
    try:
        with open(filename, 'r') as file:
            while True:
                # Read a chunk of lines
                chunk = [file.readline().strip() for _ in range(CHUNK_SIZE)]
                if not any(chunk):  # If chunk is empty, end of file reached
                    break

                # Process the chunk
                logging.info(f"Processing chunk starting at line {total_lines_processed + 1} in file: {filename}")
                process_chunk(chunk, schema_val_obj, kafka_producer_obj, topic)
                total_lines_processed += len(chunk)

        logging.info(f"Completed processing file: {filename}. Total lines processed: {total_lines_processed}")
    except Exception as e:
        logging.error(f"Error processing file '{filename}': {e}")


def process_flight_transaction_and_location_update_files():
    """Processes all files concurrently."""
    logging.info("Starting the processing of flight transactions and location update files.")
    files = [TRANSACTION_FILE_PATH, LOCATION_FILE_PATH]
    kafka_producer_obj = KafkaProducerWrapper(KAFKA_CONFIG)  # Single Kafka producer instance

    try:
        # Thread pool for processing files
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = [
                executor.submit(
                    process_file, filename, schema_dict[filename], topic_dict[filename], kafka_producer_obj
                )
                for filename in files
            ]

            # Wait for all file processing futures to complete
            for future in futures:
                future.result()

        logging.info("All files processed successfully.")
    except Exception as e:
        logging.error(f"Error in processing files: {e}")


if __name__ == "__main__":
    logging.info("Starting file processing service.")
    print("@@@@@@@@@@@2")
    print(LOG_FILE_PATH)
    process_flight_transaction_and_location_update_files()
    logging.info("File processing service completed.")
