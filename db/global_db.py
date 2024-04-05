import os
from pymongo import MongoClient
from validators.validators import validate_metadata, validate_id
from formatters.db import from_db

global_db_url = os.getenv("GLOBAL_DB_URL")
global_client = MongoClient(global_db_url)
global_db = global_client["global-db"]
global_metadata = global_db["metadata"]


def insert_metadata(metadata):
    validate_metadata(metadata)
    global_metadata.insert_one(metadata)
    validate_metadata(metadata, with_db_id=True)
    from_db(metadata)


def get_one_metadata(id):
    validate_id(id)
    metadata = global_metadata.find_one({"_id": id})
    validate_metadata(metadata, with_db_id=True)
    from_db(metadata)
    return metadata


def get_all_metadata():
    all_metadata = list(global_metadata.find({}))
    for metadata in all_metadata:
        validate_metadata(metadata, with_db_id=True)
        from_db(metadata)
    return all_metadata
