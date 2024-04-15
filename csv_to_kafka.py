import pandas as pd
from fastavro import writer, parse_schema, reader
import io
from urllib.parse import urlparse
from config.config import config
import fastavro
from utils.kafkautils import KafkaUtils
import os


class Etl_first:
    def __init__(self,csv_path,topic_name):
        self.config_instance = config.get("avroschema")
        self.avro_file = "data.avro"
        df = self.read_csv_file(csv_path)
        df = self.parse_data(df)
        self.create_avro_file(df)
        self.send_to_kafka(topic_name)
        self.delete_avro_file()
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
        print("Reading CSV file")
        # Open the CSV file
        df = pd.read_csv(csv_file_path)

        # Print the DataFrame to see its contents
        return df
    def parse_data(self,df):
        print("Parsing Data")
        df['URL'] = df['URL'].apply(self.parse_url)

        ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
        valid_ips = df['IP'].str.match(ip_pattern)
        df = df[valid_ips]

        df = df[['IP', 'Time', 'URL', 'Status']]
        df['Time'] = df['Time'].str.replace('[', '').str.strip()
        df['Time'] = pd.to_datetime(df['Time'], format='%d/%b/%Y:%H:%M:%S', errors='coerce')

        df['IP'] = df['IP'].astype(str)
        df['Time'] = df['Time'].astype(str)
        df['Status'] = df['Status'].astype(str)

        return df

    def create_avro_file(self,df):
        print("Creating Avro formate file")

        avro_schema = self.config_instance

        # Convert DataFrame to list of dictionaries
        records = df.to_dict(orient='records')

        # Convert schema to parsed schema
        parsed_schema = parse_schema(avro_schema)

        with io.BytesIO() as avro_file:
            writer(avro_file, parsed_schema, records)

            # Reset file pointer
            avro_file.seek(0)

            # Get the Avro bytes
            avro_bytes = avro_file.read()

        with open("data.avro", "wb") as f:
            f.write(avro_bytes)
    def read_avro_file(self):
        with open("data.avro", "rb") as f:
            avro_reader = reader(f)

            # Print each record in the Avro file
            for record in avro_reader:
                print(record)

    def send_to_kafka(self,topic):
        print("Sending to Kafka")
        kafka_utils = KafkaUtils()
        producer = kafka_utils.produce_data()
        # Read Avro file and produce data to Kafka
        with open(self.avro_file, 'rb') as avro_file:
            avro_reader = fastavro.reader(avro_file)
            schema = avro_reader.writer_schema

            for record in avro_reader:
                # Serialize Avro record to bytes
                avro_bytes_io = io.BytesIO()
                fastavro.schemaless_writer(avro_bytes_io, schema, record)

                # Produce serialized data to Kafka
                producer.produce(topic=topic, value=avro_bytes_io.getvalue())

        # Flush Kafka producer to ensure all messages are sent
        producer.flush()
    def delete_avro_file(self):
        if os.path.exists(self.avro_file):
            # Delete the file
            os.remove(self.avro_file)
            print(f"File '{self.avro_file}' deleted successfully.")
        else:
            print(f"File '{self.avro_file}' does not exist.")

if __name__ == '__main__':
    pass
