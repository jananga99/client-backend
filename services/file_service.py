from datetime import datetime
from classes.access_type import AccessType
from services.node_service import (
    get_chunk_data_from_node,
    get_nodes,
    random_node_assign,
    send_chunk_to_node,
    delete_chunk_from_node,
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
from bson import objectid


def split_to_chunks(file):
    chunk_size = 1024 * 32
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
    if objectid.ObjectId.is_valid(file_id):
        metadata = global_db.get_one_metadata(file_id)
        print("Got metadata from global db in get file")
    else:
        metadata = local_db.get_one_metadata(file_id)
        print("Got metadata from local db in get file")
    validate_metadata(metadata)

    # Get chunks from nodes
    got_chunk_arr = get_chunk_arr(
        metadata["start_chunk_id"], metadata["start_chunk_node_id"]
    )

    # Combine chunks
    combined_file = combine_chunks(got_chunk_arr)

    return metadata, combined_file


def get_all_public_metadata(search=""):
    all_metadata = global_db.get_all_metadata(search)
    for metadata in all_metadata:
        validate_metadata(metadata)
    return all_metadata


def get_all_private_metadata(search=""):
    all_metadata = local_db.get_all_metadata(search)
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


def delete_file(file_id):
    print("Delete file called with id " + file_id)
    # Validate file_id
    validate_metadata_id(file_id)
    print("Validated file id in delete_file")
    # Get metadata from local and global db
    if objectid.ObjectId.is_valid(file_id):
        metadata = global_db.get_one_metadata(file_id)
        print("Got metadata from global db in delete file")
    else:
        metadata = local_db.get_one_metadata(file_id)
        print("Got metadata from local db in delete file")
    validate_metadata(metadata)
    print("Validated metadata in delete_file")
    # Delete chunks
    print("Got chunk arr in delete_file")
    current_chunk_id = metadata["start_chunk_id"]
    current_chunk_node_id = metadata["start_chunk_node_id"]
    # Get available nodes
    nodes = get_nodes()
    while current_chunk_id != "" and current_chunk_node_id != "":
        chunk_data = get_chunk_data_from_node(
            nodes[int(current_chunk_node_id) - 1], current_chunk_id
        )
        validate_chunk_data(chunk_data)
        delete_chunk_from_node(nodes[int(current_chunk_node_id) - 1], current_chunk_id)
        if current_chunk_id == chunk_data["next_chunk_id"]:
            raise Error("Next chunk id is same as current chunk id", 500)
        current_chunk_id = chunk_data["next_chunk_id"]
        current_chunk_node_id = chunk_data["next_chunk_node_id"]
    print("Deleted chunks in delete_file")
    # Delete metadata from local and global db
    if objectid.ObjectId.is_valid(file_id):
        global_db.delete_metadata(file_id)
        print("Delete metadata from global db in delete file")
    else:
        local_db.delete_metadata(file_id)
        print("Delete metadata from local db in delete file")


def make_public(file_id):
    print("Make public called")
    # Validate file_id
    validate_metadata_id(file_id)
    print("Metadata id validated in make public")

    if objectid.ObjectId.is_valid(file_id):
        raise Error(
            "File id is public. Cannot make public a file that is already public", 400
        )

    # Get metadata from local db
    metadata = local_db.get_one_metadata(file_id)
    print("Metadata got from global db in make public")
    metadata["access_type"] = AccessType.PUBLIC.value
    validate_metadata(metadata)
    print("Metadata validated in make public")

    # Insert metadata into global db
    metadata = global_db.insert_metadata(metadata)
    print("Metadata got from local db in make public")

    # Delete metadata from local db
    local_db.delete_metadata(file_id)
    print("Metadata deleted fromlocal db in make public")

    return metadata


def make_private(file_id):
    print("Mae private called")
    # Validate file_id
    validate_metadata_id(file_id)
    print("Metadata id validated in make private")

    if not objectid.ObjectId.is_valid(file_id):
        raise Error(
            "File id is private. Cannot make private a file that is already private",
            400,
        )

    # Get metadata from global db
    metadata = global_db.get_one_metadata(file_id)
    print("Metadata got from global db in make private")
    metadata["access_type"] = AccessType.PRIVATE.value
    validate_metadata(metadata)
    print("Metadata validated in make private")

    # Insert metadata into local db
    metadata = local_db.insert_metadata(metadata)
    print("Metadata got from global db in make private")

    # Delete metadata from global db
    global_db.delete_metadata(file_id)
    print("Metadata deleted from global db in make private")

    return metadata


# Rename a file, check whether it is in global or local, then updates the name
def rename(file_id, new_name):
    validate_metadata_id(file_id)
    # Get metadata from local and global db
    if objectid.ObjectId.is_valid(file_id):
        metadata = global_db.get_one_metadata(file_id)
        print("Got metadata from global db in rename file")
        metadata["name"] = new_name
        validate_metadata(metadata)
        metadata = global_db.update_metadata(file_id, metadata)
    else:
        metadata = local_db.get_one_metadata(file_id)
        print("Got metadata from local db in rename file")
        metadata["name"] = new_name
        validate_metadata(metadata)
        metadata = local_db.update_metadata(file_id, metadata)
    return metadata
