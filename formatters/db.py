from schemas.schema import Metadata, Node


def from_db_metadata(metadata):
    return {
        "id": str(metadata._id),
        "name": metadata.name,
        "size": metadata.size,
        "type": metadata.type,
        "access_type": metadata.access_type,
        "merkel_root": metadata.merkel_root,
        "start_chunk_id": metadata.start_chunk_id,
        "start_chunk_node_id": metadata.start_chunk_node_id,
        "created_at": metadata.created_at,
        "lastViewed_at": metadata.lastViewed_at,
    }


def to_db_metadata(metadata):
    if "id" in metadata:
        metadata["_id"] = metadata["id"]
        del metadata["id"]
    return Metadata(**metadata)


def to_db_node(node):
    if "id" in node:
        node["_id"] = node["id"]
        del node["id"]
    return Node(**node)


def from_db_node(node):
    return {
        "id": node._id,
        "address": node.address,
        "url": node.url,
    }
