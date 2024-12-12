from app.utils.kafka_utils import KafkaProducerWrapper
from unittest.mock import patch


# Test Kafka producer
def test_kafka_producer_publish():
    kafka_config = {"bootstrap_servers": "localhost:9092"}
    producer = KafkaProducerWrapper(kafka_config)

    # Mock Kafka producer's 'produce' method
    with patch.object(producer.producer, 'produce', return_value=None) as mock_produce:
        producer.publish("test_topic", '{"key": "value"}', {"header": "test"})
        mock_produce.assert_called_once_with(
            "test_topic", value='{"key": "value"}'.encode("utf-8"), headers=[('header', 'test'.encode('utf-8'))]
        )
