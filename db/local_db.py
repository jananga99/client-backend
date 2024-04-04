import os
from pymongo import MongoClient

local_db_url = os.getenv("LOCAL_DB_URL")
local_client = MongoClient(local_db_url)
local_db = local_client["local-db"]
local_metadata = local_db["metadata"]
local_nodes = local_db["nodes"]


def insert_metadata(metadata):
    local_metadata.insert_one(metadata)
    metadata["id"] = str(metadata["_id"])
    del metadata["_id"]


def get_one_metadata(id):
    metadata = local_metadata.find_one({"_id": id})
    if metadata is not None:
        metadata["id"] = str(metadata["_id"])
        del metadata["_id"]
    return metadata


def get_all_metadata():
    all_metadata = list(local_metadata.find({}))
    for metadata in all_metadata:
        metadata["id"] = str(metadata["_id"])
        del metadata["_id"]
    return all_metadata


def get_all_nodes():
    all_nodes = list(local_nodes.find({}))
    for node in all_nodes:
        node["id"] = str(node["_id"])
        del node["_id"]
    return all_nodes
