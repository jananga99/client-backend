from db.local_db import insert_node


def add_nodes():
    for i in range(5):
        insert_node(
            {
                "id": i + 1,
                "address": f"Container {i+1}",
                "url": f"http://localhost:500{i+1}",
            }
        )


add_nodes()
