def from_db_id(val):
    val["id"] = str(val["_id"])
    del val["_id"]
    return val


def to_db_id(val):
    val["_id"] = val["id"]
    del val["id"]
    return val


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
