from pymongo import MongoClient
from datetime import datetime
import pytz

from constants import *


class Metadata:
    def __init__(self, database_connector):
        self.database_connector = database_connector

    def create_file(self, filename: str):
        timezone_london = pytz.timezone("Etc/Greenwich")
        london_time = datetime.now(timezone_london)

        metadata_file = {
            "datasetName": filename,
            "timeCreated": london_time.strftime("%Y-%m-%dT%H:%M:%S-00:00"),
            DOCUMENT_ID_NAME: METADATA_ID,
            FINISHED_FIELD_NAME: False,
            "type": "normalization"
        }
        self.database_connector.insert_one_in_file(filename, metadata_file)

    def update_finished_flag(self, filename: str, flag: str):
        metadata_new_value = {
            FINISHED_FIELD_NAME: flag,
        }
        metadata_query = {
            DOCUMENT_ID_NAME: METADATA_ID
        }
        self.database_connector.update_one(filename, metadata_new_value,
                                           metadata_query)


class Database:
    def __init__(self, database_url: str, replica_set: str,
                 database_port: str, database_name: str):
        self.__mongo_client = MongoClient(
            database_url + '/?replicaSet=' + replica_set, int(database_port))
        self.__database = self.__mongo_client[database_name]

    def find(self, filename: str, query: list) -> dict:
        file_collection = self.__database[filename]
        return file_collection.find(*query)

    def get_filenames(self) -> list:
        return self.__database.list_collection_names()

    def update_one(self, filename: str, new_value: any, query: dict):
        new_values_query = {"$set": new_value}
        file_collection = self.__database[filename]
        file_collection.update_one(query, new_values_query)

    def find_one(self, filename: str, query: dict) -> dict:
        file_collection = self.__database[filename]
        return file_collection.find_one(query)

    def insert_one_in_file(self, filename: str, document: dict):
        file_collection = self.__database[filename]
        file_collection.insert_one(document)

    @staticmethod
    def collection_database_url(
            database_url, database_name, database_filename,
            database_replica_set
    ):
        return (
                database_url
                + "/"
                + database_name
                + "."
                + database_filename
                + "?replicaSet="
                + database_replica_set
                + "&authSource=admin"
        )


class UserRequest:
    __MESSAGE_INVALID_FIELD = "invalid field"
    __MESSAGE_INVALID_FILENAME = "invalid dataset name"
    __MESSAGE_MISSING_FIELD = "missing field"
    __MESSAGE_UNFINISHED_PROCESSING = "unfinished processing in input dataset"
    __MESSAGE_DUPLICATED_FILENAME = "duplicated dataset name"
    __STRING_TYPE = "string"
    __NUMBER_TYPE = "number"

    def __init__(self, database_connector: Database):
        self.__database = database_connector

    def parent_filename_validator(self, filename: str):
        filenames = self.__database.get_filenames()

        if filename not in filenames:
            raise Exception(self.__MESSAGE_INVALID_FILENAME)

    def normalization_filename_validator(self, filename: str):
        filenames = self.__database.get_filenames()

        if filename in filenames:
            raise Exception(self.__MESSAGE_DUPLICATED_FILENAME)

    def filename_field_validator(self, filename: str, field: str):
        if not field:
            raise Exception(self.__MESSAGE_MISSING_FIELD)

        filename_metadata_query = {"datasetName": filename}

        filename_metadata = self.__database.find_one(filename,
                                                     filename_metadata_query)

        if field not in filename_metadata["fields"]:
            raise Exception(self.__MESSAGE_INVALID_FIELD)
