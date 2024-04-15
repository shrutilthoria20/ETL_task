import pymongo
import os
class MongoUtils:
    def __init__(self):
        pass
    def create_connection(self, db_name, collection_name):
        mongo_uri = os.environ['MONGO_URL']
        client = pymongo.MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        return collection

    def insert_data(self, collection, data):
        collection.insert_many(data)

