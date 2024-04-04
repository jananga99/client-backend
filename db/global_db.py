import os
from pymongo import MongoClient

global_db_url = os.getenv("GLOBAL_DB_URL")
global_client = MongoClient(global_db_url)
global_db = global_client["global-db"]
global_metadata = global_db["metadata"]


def insert_metadata(metadata):
    global_metadata.insert_one(metadata)
    metadata["id"] = str(metadata["_id"])
    del metadata["_id"]


def get_one_metadata(id):
    metadata = global_metadata.find_one({"_id": id})
    if metadata is not None:
        metadata["id"] = str(metadata["_id"])
        del metadata["_id"]
    return metadata


def get_all_metadata():
    all_metadata = list(global_metadata.find({}))
    for metadata in all_metadata:
        metadata["id"] = str(metadata["_id"])
        del metadata["_id"]
    return all_metadata
