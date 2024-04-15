import fastavro
from confluent_kafka import KafkaError, KafkaException
import io
from config.config import config
import os
from utils.kafkautils import KafkaUtils
from utils.mongoutils import MongoUtils
class KafkaToMongo:
    def __init__(self,topic,collection_name):
        self.config_instance = config.get("avroschema")
        data = self.read_data_from_kafka(topic)
        print(data)
        self.send_to_mongo(data,collection_name)

    def read_data_from_kafka(self,topic):
        print("Reading from kafka")
        data_list = []
        kafka_utils = KafkaUtils()
        consumer = kafka_utils.data_consumer()

        # Subscribe to the Kafka topic
        consumer.subscribe([topic])

        avro_schema = self.config_instance

        # Start consuming messages
        try:
            while True:
                msg = consumer.poll(timeout=1.0)  # Poll for new messages
                if msg is None:
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, commit offsets
                        print('%% %s [%d] reached end at offset %d\n' %
                              (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:

                    avro_bytes = msg.value()  # Avro data in bytes
                    avro_file = io.BytesIO(avro_bytes)  # Wrap bytes in a file-like object
                    # Deserialize Avro data
                    avro_record = fastavro.schemaless_reader(avro_file, avro_schema)
                    data_list.append(avro_record)
        except KeyboardInterrupt:
            pass
        finally:
            # Close the Kafka consumer
            consumer.close()
            return data_list


    def send_to_mongo(self,data,collection_name):
        print("Sending to Mongo")
        mongo_utils = MongoUtils()
        collection = mongo_utils.create_connection(os.environ['DB_NAME'], collection_name)
        mongo_utils.insert_data(collection,data)

if __name__ == '__main__':
    pass
