import json
import logging
from jsonschema import validate, ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class SchemaValidator:
    def __init__(self, schema_file_path):
        """Initialize with the schema file path."""
        self.schema_file_path = schema_file_path
        logging.info(f"SchemaValidator initialized with schema file path: {self.schema_file_path}")

    def __load_schema_file(self):
        """Loads the JSON schema from the provided file path."""
        try:
            logging.info(f"Loading schema file: {self.schema_file_path}")
            with open(self.schema_file_path, 'r') as file:
                schema = json.load(file)
            logging.info(f"Schema file loaded successfully from {self.schema_file_path}")
            return schema
        except FileNotFoundError as e:
            logging.error(f"Schema file not found: {self.schema_file_path}")
            raise e
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON in schema file: {self.schema_file_path}. Error: {e}")
            raise e
        except Exception as e:
            logging.error(f"Unexpected error loading schema file {self.schema_file_path}: {e}")
            raise e

    def validate_json(self, json_data):
        """Validates the given JSON data against the schema."""
        try:
            logging.info("Validating JSON data against schema.")
            # Validate the JSON data
            validate(instance=json_data, schema=self.__load_schema_file())
            logging.info("JSON data is valid according to the schema.")
            return True
        except ValidationError as err:
            logging.error(f"Validation failed: JSON data is invalid. Error: {err.message}")
        except Exception as e:
            logging.error(f"Unexpected error during validation: {e}")
        return False


# Example of how this would work
# if __name__ == "__main__":
#     file_path = 'data/locations.json'
#     schema_validator = SchemaValidator('app/schemas/location_schema.json')
#     with open(file_path, 'r') as file:
#         json_data = json.load(file)
#         schema_validator.validate_json(json_data)
