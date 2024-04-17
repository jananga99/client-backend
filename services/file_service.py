from datetime import datetime
from classes.access_type import AccessType
from services.node_service import (
    get_chunk_data_from_node,
    get_nodes,
    random_node_assign,
    send_chunk_to_node,
)
import db.global_db as global_db
import db.local_db as local_db
from validators.validators import (
    validate_chunk,
    validate_file,
    validate_access_type,
    validate_node,
    validate_metadata,
    validate_metadata_id,
    validate_chunk_data,
)
from exceptions.error import Error


def split_to_chunks(file):
    chunk_size = 1024
    chunks = []
    file_size = 0
    while True:
        chunk = file.read(chunk_size)
        validate_chunk(chunk)
        if not chunk:
            break
        chunks.append(chunk)
        file_size += len(chunk)
    return chunks, file_size


def combine_chunks(chunks):
    combined_data = b"".join(
        chunk.encode("utf-8") if isinstance(chunk, str) else chunk for chunk in chunks
    )
    return combined_data


def upload_file(file, access_type):

    # Validae file and access_type
    validate_file(file)
    validate_access_type(access_type)

    # Split file into chunks
    chunks, file_size = split_to_chunks(file)

    # Get available nodes
    nodes = get_nodes()

    # Assign chunks to nodes randomly
    assigned_chunk_data = random_node_assign(chunks, nodes)

    # Create metadata
    merkel_root = "Dummy Merkel Root"
    start_chunk_id = assigned_chunk_data[0]["id"]
    start_chunk_node_id = assigned_chunk_data[0]["node_id"]
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_viewed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    metadata = {
        "name": file.filename,
        "size": file_size,
        "type": file.content_type,
        "access_type": access_type,
        "merkel_root": merkel_root,
        "start_chunk_id": start_chunk_id,
        "start_chunk_node_id": start_chunk_node_id,
        "created_at": created_at,
        "lastViewed_at": last_viewed_at,
    }

    # Insert metadata into local and global db
    if access_type == AccessType.PUBLIC.value:
        metadata = global_db.insert_metadata(metadata)
    else:
        metadata = local_db.insert_metadata(metadata)
    validate_metadata(metadata)

    # Send chunks to nodes
    for chunk_data in assigned_chunk_data:
        node = nodes[chunk_data["node_id"] - 1]
        validate_node(node)
        send_chunk_to_node(node, chunk_data)

    return metadata


def get_file(file_id):
    # Validate file_id
    validate_metadata_id(file_id)

    # Get metadata from local and global db
    try:
        metadata = local_db.get_one_metadata(file_id)
    except Error:
        metadata = global_db.get_one_metadata(file_id)
    validate_metadata(metadata)

    # Get chunks from nodes
    got_chunk_arr = get_chunk_arr(
        metadata["start_chunk_id"], metadata["start_chunk_node_id"]
    )

    # Combine chunks
    combined_file = combine_chunks(got_chunk_arr)

    return metadata, combined_file


def get_all_public_metadata():
    all_metadata = global_db.get_all_metadata()
    for metadata in all_metadata:
        validate_metadata(metadata)
    return all_metadata


def get_all_private_metadata():
    all_metadata = local_db.get_all_metadata()
    for metadata in all_metadata:
        validate_metadata(metadata)
    return all_metadata


def get_chunk_arr(start_chunk_id, start_chunk_node_id):
    # Get available nodes
    nodes = get_nodes()

    # Get chunks from nodes
    current_chunk_id = start_chunk_id
    current_chunk_node_id = start_chunk_node_id
    got_chunk_arr = []
    while current_chunk_id != "" and current_chunk_node_id != "":
        chunk_data = get_chunk_data_from_node(
            nodes[int(current_chunk_node_id) - 1], current_chunk_id
        )
        validate_chunk_data(chunk_data)
        got_chunk_arr.append(chunk_data["chunk"])
        if current_chunk_id == chunk_data["next_chunk_id"]:
            raise Error("Next chunk id is same as current chunk id", 500)
        current_chunk_id = chunk_data["next_chunk_id"]
        current_chunk_node_id = chunk_data["next_chunk_node_id"]

    return got_chunk_arr


def test_split_combine_file(file):
    # Split file into chunks
    chunks, file_size = split_to_chunks(file)

    # Combine chunks
    combined_file = combine_chunks(chunks)

    return combined_file
