import random
import uuid
import requests
import db.local_db as local_db
from validators.validators import validate_metadata, validate_id, validate_node


def get_nodes():
    nodes = local_db.get_all_nodes()
    for node in nodes:
        validate_node(node, with_id=True)
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
    # TODO - Remove this line
    if url != "http://localhost:5001":
        url = "http://localhost:5001"
    response = requests.post(
        url + "/chunk",
        data={
            "id": chunk_data["id"],
            "next_chunk_id": chunk_data.get("next_chunk_id") or "",
            "next_chunk_node_id": chunk_data.get("next_chunk_node_id") or "",
            "chunk": chunk_data["chunk"],
        },
    )
    return response.status_code == 200


def get_chunk_data_from_node(node, chunk_id):
    url = node["url"]
    # TODO - Remove this line
    if url != "http://localhost:5001":
        url = "http://localhost:5001"
    response = requests.get(url + "/chunk/" + chunk_id)
    return response.json()
