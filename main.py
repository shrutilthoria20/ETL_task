from csv_to_kafka import Etl_first
from kafka_to_mongo import Etl_second

Etl_first(r'E:\archive\weblog.csv','my-topic')
Etl_second('my-topic')