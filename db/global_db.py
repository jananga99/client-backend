import os
from exceptions.error import Error
from pymongo import MongoClient
from validators.validators import validate_metadata, validate_metadata_id
from bson import ObjectId
from formatters.db import (
    from_global_db_metadata,
)

global_db_url = os.getenv("GLOBAL_DB_URL")
global_client = MongoClient(global_db_url)
global_db = global_client["global-db"]
global_metadata = global_db["metadata"]


def insert_metadata(metadata):
    validate_metadata(metadata, with_id=False)
    global_metadata.insert_one(metadata)
    metadata = from_global_db_metadata(metadata)
    validate_metadata(metadata)
    return metadata


def get_one_metadata(id):
    validate_metadata_id(id)
    metadata = global_metadata.find_one({"_id": ObjectId(id)})
    if metadata is None:
        raise Error(f"Metadata with id: {id} not found", 404)
    metadata = from_global_db_metadata(metadata)
    validate_metadata(metadata)
    return metadata


def delete_metadata(id):
    validate_metadata_id(id)
    metadata = global_metadata.find_one({"_id": ObjectId(id)})
    if metadata is None:
        raise Error(f"Metadata with id: {id} not found", 404)
    global_metadata.delete_one({"_id": ObjectId(id)})


def get_all_metadata(search=""):
    query = {}
    if search:
        query["name"] = {"$regex": search, "$options": "i"}
    all_metadata = list(global_metadata.find(query))
    for i in range(len(all_metadata)):
        all_metadata[i] = from_global_db_metadata(all_metadata[i])
        validate_metadata(all_metadata[i])
    return all_metadata


def update_metadata(id, metadata):
    validate_metadata_id(id)
    validate_metadata(metadata)
    global_metadata.update_one({"_id": ObjectId(id)}, {"$set": metadata})
    metadata = global_metadata.find_one({"_id": ObjectId(id)})
    metadata = from_global_db_metadata(metadata)
    validate_metadata(metadata)
    return metadata
