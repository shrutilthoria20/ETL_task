config={}

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


