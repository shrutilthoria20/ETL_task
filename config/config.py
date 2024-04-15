import os

config = {}

config["avroschema"] = {
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

config["producer_config"] = {'bootstrap.servers': os.environ['BOOTSTRAP_SERVER']}

config["consumer_config"] = {
            'bootstrap.servers': os.environ['BOOTSTRAP_SERVER'],
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'  # Start reading from the beginning of the topic
        }
