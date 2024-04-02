from datetime import datetime
import random
from flask import Flask, request, jsonify
from pymongo import MongoClient
from enum import Enum
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Connect to MongoDB
local_db_url = os.getenv("LOCAL_DB_URL")
global_db_url = os.getenv("GLOBAL_DB_URL") 
global_client = MongoClient(global_db_url)
global_db = global_client['global']
global_metadata = global_db['metadata']

local_client = MongoClient(local_db_url)
local_db = local_client['local-db']
local_metadata = local_db['metadata']
local_nodes = local_db['nodes']


class AccessType(Enum):
    PUBLIC = "public"
    PRIVATE = "private"


class Metadata:
    def __init__(self, id, name, size, type, access_type, merkel_root, start_chunk_id, start_chunk_node_id, created_at, lastViewed_at):
        self.id = id
        self.name = name
        self.size = size
        self.type = type
        self.access_type = access_type
        self.merkel_root = merkel_root
        self.start_chunk_id = start_chunk_id
        self.start_chunk_node_id = start_chunk_node_id
        self.created_at = created_at
        self.lastViewed_at = lastViewed_at

@app.route('/file', methods=['POST'])
def upload_file():

    # try:
        file = request.files['file']
        access_type = request.form['accessType']

        chunks = split_to_chunks(file)
        nodes = get_nodes()

        assigned_chunk_data = random_node_assign(chunks, nodes)

        # Build the merkel tree
        merkel_root = "Dummy Merkel Root"
    
        start_chunk_id = "1"
        start_chunk_node_id = "1"
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
        "lastViewed_at": last_viewed_at
        }

        if access_type == AccessType.PUBLIC.value:
            metadata['global_id'] = add_to_global_db(metadata)
        elif access_type != AccessType.PRIVATE.value:
            return jsonify({'success': False, 'process': 'Invalid access type.'})
        add_to_local_db(metadata)

        

        return jsonify({'success': True, 'process': 'File uploaded successfully.'})
    # except:
    #     print("An error occured")

        return jsonify({'fail': True, 'process': 'Unexpected error occured'})

   

def add_to_global_db(metadata):
    result = global_metadata.insert_one(metadata)
    return result.inserted_id

def add_to_local_db(metadata):
    result = local_metadata.insert_one(metadata)
    return result.inserted_id

def split_to_chunks(file):
    chunk_size = 1024
    chunks = []
    while True:
        chunk = file.read(chunk_size)
        if not chunk:
            break
        chunks.append(chunk)
    return chunks

def get_nodes():
    return list(local_nodes.find({}))

def random_node_assign(chunks, nodes):
    assigned_chunk_data = [None for _ in range(len(chunks))]
    for i, chunk in enumerate(chunks):
        node_id = random.randint(1, len(nodes)) 
        chunk_id = i
        assigned_chunk_data[i] ={
            "id": chunk_id,
            "node_id": node_id,
            "chunk": chunk
        }
        if i>0:
            assigned_chunk_data[i-1]["next_chunk_id"] = chunk_id
            assigned_chunk_data[i-1]["next_chunk_node_id"] = node_id
    return assigned_chunk_data

def send_to_node(node, chunk_data):
    url = node.url  
    response = requests.post(url + '/chunk', json=chunk_data)
    if response.status_code == 200:
        print(f"Chunk sent successfully to node at {url}.")
    else:
        print(f"Failed to send chunk to node at {url}. Status code: {response.status_code}")

if __name__ == '__main__':
    app.run(debug=True,port=int(os.getenv('PORT', 5000)))  # Run the Flask app in debug mode
