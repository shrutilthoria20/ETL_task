import os
from confluent_kafka import Producer, Consumer

class KafkaUtils:
    def __init__(self):
        # self.bootstrap_servers = "os.environ['BOOTSTRAP_SERVER']"
        self.bootstrap_servers = "localhost:9092"
    def produce_data(self):
        # Kafka producer configuration
        producer_config = {
            'bootstrap.servers': self.bootstrap_servers
        }
        # Create Kafka producer
        producer = Producer(producer_config)
        return producer

    def data_consumer(self):
        # Kafka consumer configuration
        consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
        }
        # Create Kafka consumer
        consumer = Consumer(consumer_config)
        return consumer

