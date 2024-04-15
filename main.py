import pandas as pd
from fastavro import writer, parse_schema, reader
import io
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import fastavro
from urllib.parse import urlparse
import pymongo
from config.config import Config


class Etl:
    def __init__(self):
        self.config_instance = Config()
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

        avro_schema = self.config_instance.all_schema()

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

        # Now, you can save the avro_bytes to a file or use it as needed
        # For example, you can write it to a file like this:
        with open("data.avro", "wb") as f:
            f.write(avro_bytes)

if __name__ == '__main__':
    obj = Etl()
    df = obj.read_csv_file(r"E:\archive\weblog.csv")
    df = obj.parse_data(df)
    obj.create_avro_file(df)
