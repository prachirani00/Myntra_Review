from pymongo import MongoClient
import pandas as pd


class MongoDBClient:
    def __init__(self, client_url: str, database_name: str):
        self.client = MongoClient(client_url)
        self.db = self.client[database_name]

    def bulk_insert(self, dataframe: pd.DataFrame, collection_name: str):
        if dataframe.empty:
            return
        records = dataframe.to_dict(orient="records")
        self.db[collection_name].insert_many(records)

    def find(self, collection_name: str):
        collection = self.db[collection_name]
        documents = list(collection.find({}, {"_id": False}))  # exclude MongoDB’s internal _id
        return pd.DataFrame(documents)


def mongo_operation(client_url: str, database_name: str):
    return MongoDBClient(client_url=client_url, database_name=database_name)
