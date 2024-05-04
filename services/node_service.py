import random
import uuid
import requests
import db.local_db as local_db
from validators.validators import validate_node
from exceptions.error import Error
from formatters.chunk import to_byte_chunk, to_str_chunk


def get_nodes():
    nodes = local_db.get_all_nodes()
    for node in nodes:
        validate_node(node)
    return nodes


def random_node_assign(chunks, nodes):
    assigned_chunk_data = [None for _ in range(len(chunks))]
    for i, chunk in enumerate(chunks):
        node_id = random.randint(1, len(nodes))
        chunk_id = str(uuid.uuid4())
        assigned_chunk_data[i] = {"id": chunk_id, "node_id": node_id, "chunk": chunk}
        if i > 0:
            assigned_chunk_data[i - 1]["next_chunk_id"] = chunk_id
            assigned_chunk_data[i - 1]["next_chunk_node_id"] = node_id
    return assigned_chunk_data


def send_chunk_to_node(node, chunk_data):
    url = node["url"]
    # if url != "http://localhost:5001":
    #     url = "http://localhost:5001"
    response = requests.post(
        url + "/chunk",
        json={
            "id": chunk_data["id"],
            "next_chunk_id": chunk_data.get("next_chunk_id") or "",
            "next_chunk_node_id": chunk_data.get("next_chunk_node_id") or "",
            "chunk": to_str_chunk(chunk_data["chunk"]),
        },
    )
    if response.status_code != 200:
        message = response.json().get("message") or "Error sending chunk to node"
        raise Error(message, 500)

def delete_chunk_from_node(node, chunk_id):
    url = node["url"]
    # if url != "http://localhost:5001":
    #     url = "http://localhost:5001"
    response = requests.delete(url + "/chunk/" + chunk_id)
    if response.status_code != 200:
        message = response.json().get("message") or "Error deleting chunk from node"
        raise Error(message, 500)


def get_chunk_data_from_node(node, chunk_id):
    url = node["url"]
    # if url != "http://localhost:5001":
    #     url = "http://localhost:5001"
    response = requests.get(url + "/chunk/" + chunk_id)
    if response.status_code != 200:
        message = response.json().get("message") or "Error getting chunk from node"
        raise Error(message, 500)
    chunk_data = response.json()
    chunk_data["chunk"] = to_byte_chunk(chunk_data["chunk"])
    return chunk_data
