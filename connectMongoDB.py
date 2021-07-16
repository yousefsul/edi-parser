import datetime
from pymongo import MongoClient

MONGO_CLIENT = "mongodb://yousef:Ys2021xch@209.151.150.58:63327/?authSource=admin&readPreference=primary&appname" \
               "=MongoDB%20Compass&ssl=false"


class ConnectMongoDB:
    """
    connect to devDB and client database
    define the clients_collection,visits_collection,claims_collection as none
    """

    def __init__(self):
        try:
            self.mongo_client = MongoClient(MONGO_CLIENT)
            self.db = self.mongo_client.client_2731928905_DB
            self.test_837_collection = None
            self.test_837_index_collection = None
        except ConnectionError:
            print(ConnectionError, "connection error have been occured")

    def connect_to_test_837_collection(self):
        self.test_837_collection = self.db['837_dict_coll']

    def insert_to_test_837_collection(self, result):
        try:
            self.test_837_collection.insert(result)
        except Exception as e:
            print("An Exception occurred ", e)

    def connect_837_index_collection(self):
        self.test_837_index_collection = self.db.index_coll

    def insert_837_index_collection(self, result):
        try:
            self.test_837_index_collection.insert(result)
        except Exception as e:
            print("An Exception occurred ", e)


