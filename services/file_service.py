from datetime import datetime
from classes.access_type import AccessType
from services.node_service import (
    get_chunk_from_node,
    get_nodes,
    random_node_assign,
    send_chunk_to_node,
)
import db.global_db as global_db
import db.local_db as local_db


def split_to_chunks(file):
    chunk_size = 1024
    chunks = []
    while True:
        chunk = file.read(chunk_size)
        if not chunk:
            break
        chunks.append(chunk)
    return chunks


def combine_chunks(chunks):
    combined_data = b"".join(
        chunk.encode("utf-8") if isinstance(chunk, str) else chunk for chunk in chunks
    )
    return combined_data


def upload_file(file, access_type):
    chunks = split_to_chunks(file)
    nodes = get_nodes()
    assigned_chunk_data = random_node_assign(chunks, nodes)
    merkel_root = "Dummy Merkel Root"
    start_chunk_id = assigned_chunk_data[0]["id"]
    start_chunk_node_id = assigned_chunk_data[0]["node_id"]
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_viewed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata = {
        "name": file.filename,
        "size": file.content_length,
        "type": file.content_type,
        "access_type": access_type,
        "merkel_root": merkel_root,
        "start_chunk_id": start_chunk_id,
        "start_chunk_node_id": start_chunk_node_id,
        "created_at": created_at,
        "lastViewed_at": last_viewed_at,
    }
    if access_type == AccessType.PUBLIC.value:
        global_db.insert_metadata(metadata)
        metadata["global_id"] = metadata["id"]
    elif access_type != AccessType.PRIVATE.value:
        raise Exception("Invalid access type")
    local_db.insert_metadata(metadata)
    metadata["id"] = metadata.get("global_id") or metadata["id"]
    for chunk_data in assigned_chunk_data:
        node = nodes[chunk_data["node_id"] - 1]
        send_chunk_to_node(node, chunk_data)
    return metadata


def get_file(file_id):
    metadata = local_db.get_one_metadata(file_id)
    if metadata is None:
        metadata = global_db.get_one_metadata(file_id)
    got_chunk_arr = get_chunk_arr(
        metadata["start_chunk_id"], metadata["start_chunk_node_id"]
    )
    combined_file = combine_chunks(got_chunk_arr)
    return metadata, combined_file


def get_all_metadata():
    all_metadata = list(local_db.get_all_metadata())
    all_metadata.extend(list(global_db.get_all_metadata()))
    return all_metadata


def get_chunk_arr(start_chunk_id, start_chunk_node_id):
    nodes = get_nodes()
    current_chunk_id = start_chunk_id
    current_chunk_node_id = start_chunk_node_id
    got_chunk_data_arr = []
    while current_chunk_id != "" and current_chunk_node_id != "":
        chunk_data = get_chunk_from_node(
            nodes[int(current_chunk_node_id) - 1], current_chunk_id
        )
        got_chunk_data_arr.append(chunk_data)
        if current_chunk_id == chunk_data["next_chunk_id"]:
            raise Exception("Infinite loop detected")
        current_chunk_id = chunk_data["next_chunk_id"]
        current_chunk_node_id = chunk_data["next_chunk_node_id"]
    got_chunk_arr = [chunk_data["chunk"] for chunk_data in got_chunk_data_arr]
    return got_chunk_arr
