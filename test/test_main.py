import pytest
from app.main import process_flight_transaction_and_location_update_files


# Test the integration of the main function
def test_main_processing():
    try:
        process_flight_transaction_and_location_update_files()
    except Exception as e:
        pytest.fail(f"Main process failed: {e}")
