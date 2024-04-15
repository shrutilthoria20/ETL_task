from confluent_kafka import Producer, Consumer
from config.config import config

class KafkaUtils:
    def __init__(self):
        pass

    def produce_data(self):
        # Kafka producer configuration
        producer_config = config.get('producer_config')
        # Create Kafka producer
        producer = Producer(producer_config)
        return producer

    def data_consumer(self):
        # Kafka consumer configuration
        consumer_config = config.get('consumer_config')
        # Create Kafka consumer
        consumer = Consumer(consumer_config)
        return consumer

