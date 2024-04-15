import pandas as pd
from fastavro import writer, parse_schema,reader
import io
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import fastavro
from urllib.parse import urlparse
import pymongo


class Extract:
    def __init__(self):
        pass

    def set_avro_schema(self):
        return {
            "type": "record",
            "name": "Request",
            "fields": [
                {"name": "IP", "type": "string"},
                {"name": "Time", "type": "string"},
                {"name": "URL", "type": {"type": "array", "items": {
                    "type": "record",
                    "name": "URLComponents",
                    "fields": [
                        {"name": "Scheme", "type": "string"},
                        {"name": "Netloc", "type": "string"},
                        {"name": "Path", "type": "string"},
                        {"name": "Params", "type": "string"},
                        {"name": "Query", "type": "string"},
                        {"name": "Fragment", "type": "string"}
                    ]
                }}},
                {"name": "Status", "type": "string"}
            ]
        }

    def parse_url(self,url):
        parsed_url = urlparse(url)
        url_components = {
            "Scheme": parsed_url.scheme,
            "Netloc": parsed_url.netloc,
            "Path": parsed_url.path,
            "Params": parsed_url.params,
            "Query": parsed_url.query,
            "Fragment": parsed_url.fragment
        }
        return [url_components]

    def read_csv_file(self, csv_file_path):
        # Open the CSV file
        df = pd.read_csv(csv_file_path)

        # Print the DataFrame to see its contents
        return df

    def parse_data(self,df):
        # req_pattern = r'^([A-Z]+)'
        # df['Request_Type'] = df['URL'].str.extract(req_pattern)
        # pattern = r'^[A-Z]+\s+([^ ]+)'
        # df['Request_URL'] = df['URL'].str.extract(pattern)
        df['URL'] = df['URL'].apply(self.parse_url)

        ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
        valid_ips = df['IP'].str.match(ip_pattern)
        df = df[valid_ips]
        # df = df.drop(columns=['URL'])
        df = df[['IP', 'Time', 'URL', 'Status']]
        df['Time'] = df['Time'].str.replace('[', '').str.strip()
        df['Time'] = pd.to_datetime(df['Time'], format='%d/%b/%Y:%H:%M:%S', errors='coerce')
        return df

    def convert_to_avro(self,df):
        df['IP'] = df['IP'].astype(str)
        df['Time'] = df['Time'].astype(str)
        # df['URL'] = df['URL'].astype(dict)
        df['Status'] = df['Status'].astype(str)
        # df['URL'] = df['URL'].apply(self.parse_url)


        avro_schema = self.set_avro_schema()

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')
        # for i in records:
        #     print(i['URL'])
        # Convert schema to parsed schema
        parsed_schema = parse_schema(avro_schema)

        with io.BytesIO() as avro_file:
            writer(avro_file, parsed_schema, records)

            # Reset file pointer
            avro_file.seek(0)

            # Get the Avro bytes
            avro_bytes = avro_file.read()

        # Now, you can save the avro_bytes to a file or use it as needed
        # For example, you can write it to a file like this:
        with open("data.avro", "wb") as f:
            f.write(avro_bytes)

    def read_avro_file(self):
        with open("data.avro", "rb") as f:
            avro_reader = reader(f)

            # Print each record in the Avro file
            for record in avro_reader:
                print(record)

    def send_to_kafka(self):
        # Kafka configuration
        bootstrap_servers = 'localhost:9092'
        topic = 'my-topic'

        # Avro file path
        avro_file_path = 'data.avro'

        # Kafka producer configuration
        producer_config = {
            'bootstrap.servers': bootstrap_servers
        }

        # Create Kafka producer
        producer = Producer(producer_config)

        # Read Avro file and produce data to Kafka
        with open(avro_file_path, 'rb') as avro_file:
            avro_reader = fastavro.reader(avro_file)
            schema = avro_reader.writer_schema

            for record in avro_reader:
                # Serialize Avro record to bytes
                avro_bytes_io = io.BytesIO()
                fastavro.schemaless_writer(avro_bytes_io, schema, record)

                # Produce serialized data to Kafka
                producer.produce(topic, value=avro_bytes_io.getvalue())

            # for record in avro_reader:
                # Serialize Avro record to JSON
                # json_data = json.dumps(record)

                # Produce JSON data to Kafka
                # producer.produce(topic, value=json_data.encode('utf-8'))

        # Flush Kafka producer to ensure all messages are sent
        producer.flush()

    def read_data_from_kafka(self):
        bootstrap_servers = 'localhost:9092'
        topic = 'my-topic'

        # Kafka consumer configuration
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
        }

        # Create Kafka consumer
        consumer = Consumer(consumer_config)
        data_list=[]
        # Subscribe to the Kafka topic
        consumer.subscribe([topic])

        avro_schema = self.set_avro_schema()

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
                    # Process the message
                    # print('Received message: {}'.format(
                    #     msg.value().decode('utf-8')))  # Assuming the value is encoded as UTF-8
                    # json_data = json.loads(msg.value().decode('utf-8'))
                    # data_list.append(json_data)
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

    def send_to_mongo(self,list_of_json):
        
        client = pymongo.MongoClient(mongo_uri)

        db = client["IBM_Project"]

        etl_task = db["etl_task"]

        etl_task.insert_many(list_of_json)

        print("Data Sent")


obj = Extract()
df = obj.read_csv_file(r"E:\archive\weblog.csv")
df = obj.parse_data(df)
obj.convert_to_avro(df)
obj.read_avro_file()
obj.send_to_kafka()
read_data = obj.read_data_from_kafka()
obj.send_to_mongo(read_data)
