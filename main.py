from csv_to_kafka import CsvToKafka
from kafka_to_mongo import KafkaToMongo

CsvToKafka(r'E:\archive\weblog.csv', 'my-topic')
KafkaToMongo('my-topic', 'etl_task')
