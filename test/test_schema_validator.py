from app.utils.schema_validator import SchemaValidator
import json


# Test valid and invalid schema
def test_valid_json():
    schema_validator = SchemaValidator('app/schemas/location_schema.json')
    valid_data = '{"location": "NYC", "id": 1}'
    assert schema_validator.validate_json(json.loads(valid_data))  # Should be valid


def test_invalid_json():
    schema_validator = SchemaValidator('app/schemas/location_schema.json')
    invalid_data = '{"invalid_key": "value"}'
    assert not schema_validator.validate_json(json.loads(invalid_data))  # Should be invalid
