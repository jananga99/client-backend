import os
from pymongo import MongoClient
from validators.validators import validate_metadata, validate_id, validate_node
from formatters.db import from_db, to_db

local_db_url = os.getenv("LOCAL_DB_URL")
local_client = MongoClient(local_db_url)
local_db = local_client["local-db"]
local_metadata = local_db["metadata"]
local_nodes = local_db["nodes"]


def insert_metadata(metadata):
    validate_metadata(metadata)
    local_metadata.insert_one(metadata)
    validate_metadata(metadata, with_db_id=True)
    from_db(metadata)


def get_one_metadata(id):
    validate_id(id)
    metadata = local_metadata.find_one({"_id": id})
    validate_metadata(metadata, with_db_id=True)
    from_db(metadata)
    return metadata


def get_all_metadata():
    all_metadata = list(local_metadata.find({}))
    for metadata in all_metadata:
        validate_metadata(metadata, with_db_id=True)
        from_db(metadata)
    return all_metadata

def insert_node(node):
    validate_node(node, with_id=True)
    to_db(node)
    local_nodes.insert_one(node)
    validate_node(node, with_db_id=True)
    from_db(node)

def get_all_nodes():
    all_nodes = list(local_nodes.find({}))
    for node in all_nodes:
        validate_node(node, with_db_id=True)
        from_db(node)
    return all_nodes
