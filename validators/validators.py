from datetime import datetime
from exceptions.error import Error
from classes.access_type import AccessType
from bson import ObjectId


def validate_metadata(metadata, with_id=False, with_db_id=False, with_global_id=False):
    if type(metadata) is not dict:
        raise Error("Metadata is not a dictionary", 500)
    if with_id and "id" not in metadata:
        raise Error("id is required in metadata", 500)
    if with_db_id and "_id" not in metadata:
        raise Error("_id is required in metadata", 500)
    if with_global_id and "global_id" not in metadata:
        raise Error("global_id is required in metadata", 500)
    if "name" not in metadata:
        raise Error("name is required in metadata", 500)
    if "size" not in metadata:
        raise Error("size is required in metadata", 500)
    if "type" not in metadata:
        raise Error("type is required in metadata", 500)
    if "access_type" not in metadata:
        raise Error("access_type is required in metadata", 500)
    if "merkel_root" not in metadata:
        raise Error("merkel_root is required in metadata", 500)
    if "start_chunk_id" not in metadata:
        raise Error("start_chunk_id is required in metadata", 500)
    if "start_chunk_node_id" not in metadata:
        raise Error("start_chunk_node_id is required in metadata", 500)
    if "created_at" not in metadata:
        metadata["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "lastViewed_at" not in metadata:
        metadata["lastViewed_at"] = metadata["created_at"]
    validate_access_type(metadata["access_type"])
    return metadata


def validate_id(id):
    if type(id) is not str and type(id) is not ObjectId:
        raise Error(f"id: {id} is not a string, but {type(id)}", 500)
    return id


def validate_node(node, with_id=False, with_db_id=False):
    if type(node) is not dict:
        raise Error("Node is not a dictionary", 500)
    if with_id and "id" not in node:
        raise Error("id is required in node", 500)
    if with_db_id and "_id" not in node:
        raise Error("_id is required in node", 500)
    if "address" not in node:
        raise Error("address is required in node", 500)
    if "url" not in node:
        raise Error("url is required in node", 500)
    return node


# TODO - Add validators for chunk
def validate_chunk(chunk):
    return chunk


# TODO - Add validators for file
def validate_file(file):
    return file


def validate_access_type(access_type):
    if access_type not in [AccessType.PUBLIC.value, AccessType.PRIVATE.value]:
        raise Error("Invalid access type", 500)
    return access_type


def validate_chunk_data(chunk_data):
    if type(chunk_data) is not dict:
        raise Error("Chunk data is not a dictionary", 500)
    if "id" not in chunk_data:
        raise Error("id is required in chunk data", 500)
    if "chunk" not in chunk_data:
        raise Error("chunk is required in chunk data", 500)
    else:
        validate_chunk(chunk_data["chunk"])
    if "next_chunk_id" not in chunk_data:
        raise Error("next_chunk_id is required in chunk data", 500)
    if "next_chunk_node_id" not in chunk_data:
        raise Error("next_chunk_node_id is required in chunk data", 500)
    return chunk_data
