from datetime import datetime
import io
import random
from bson import ObjectId
from flask import Flask, request, jsonify, send_file
from pymongo import MongoClient
from enum import Enum
import requests
import os
import uuid
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
    
        start_chunk_id = assigned_chunk_data[0]['id']
        start_chunk_node_id = assigned_chunk_data[0]['node_id']
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
        stored_file_id=None
        if access_type == AccessType.PUBLIC.value:
            metadata['global_id'] = add_to_global_db(metadata)
            stored_file_id = metadata['global_id']
        elif access_type != AccessType.PRIVATE.value:
            return jsonify({'success': False, 'process': 'Invalid access type.'})
        local_file_id = add_to_local_db(metadata)
        stored_file_id = stored_file_id or local_file_id
        for chunk_data in assigned_chunk_data:
            node = nodes[chunk_data['node_id']-1]
            send_chunk_to_node(node, chunk_data)

        

        return jsonify({'success': True, "file_id": stored_file_id, "name": file.filename, "size": file.content_length, "type": file.content_type, "access_type": access_type, "created_at": created_at, "lastViewed_at": last_viewed_at})
    # except:
    #     print("An error occured")

        return jsonify({'fail': True, 'process': 'Unexpected error occured'})

@app.route('/file/<file_id>', methods=['GET'])
def get_file(file_id):
    file_id = ObjectId(file_id) 
    nodes = get_nodes()
    metadata = local_metadata.find_one({"_id": file_id})
    if metadata is None:
        metadata = global_metadata.find_one({"_id": file_id})
    metadata['_id'] = str(metadata['_id'])
    current_chunk_id = metadata['start_chunk_id']
    current_chunk_node_id = metadata['start_chunk_node_id']
    got_chunk_data_arr = []
    while current_chunk_id != "" and current_chunk_node_id != "":
        chunk_data_response = get_chunk_from_node(nodes[int(current_chunk_node_id)-1], current_chunk_id)
        chunk_data = chunk_data_response['data']
        if chunk_data_response["success"]:
            got_chunk_data_arr.append(chunk_data)
            if current_chunk_id == chunk_data['next_chunk_id']:
                raise Exception("Infinite loop detected")
            current_chunk_id = chunk_data['next_chunk_id']
            current_chunk_node_id = chunk_data['next_chunk_node_id']
        else:
            raise Exception(chunk_data_response["message"])

    print(f"Length : {len(got_chunk_data_arr)}")
    got_chunk_arr = [chunk_data['chunk'] for chunk_data in got_chunk_data_arr]
    print(got_chunk_arr[0])
    combined_file = combine_chunks(got_chunk_arr)
    # return jsonify(metadata)  
    data_stream = io.BytesIO(combined_file)
    return send_file(
        data_stream,
        as_attachment=True,
        download_name=metadata['name'],
        mimetype=metadata['type']
    )

@app.route('/file', methods=['GET'])
def get_all_metadata():
    all_metadata = list(local_metadata.find({}))
    all_metadata.extend(list(global_metadata.find({})))
    for metadata in all_metadata:
        metadata['_id'] = str(metadata['_id'])
        if metadata['_id']=="660e72c6b8168fe543619721":
            print("FOUND")
    return jsonify(all_metadata)

def add_to_global_db(metadata):
    result = global_metadata.insert_one(metadata)
    return str(result.inserted_id)

def add_to_local_db(metadata):
    result = local_metadata.insert_one(metadata)
    return str(result.inserted_id)

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
    combined_data = b''.join(chunk.encode('utf-8') if isinstance(chunk, str) else chunk for chunk in chunks)
    return combined_data

def get_nodes():
    return list(local_nodes.find({}))

def random_node_assign(chunks, nodes):
    assigned_chunk_data = [None for _ in range(len(chunks))]
    for i, chunk in enumerate(chunks):
        node_id = random.randint(1, len(nodes)) 
        chunk_id = str(uuid.uuid4())
        assigned_chunk_data[i] ={
            "id": chunk_id,
            "node_id": node_id,
            "chunk": chunk
        }
        if i>0:
            assigned_chunk_data[i-1]["next_chunk_id"] = chunk_id
            assigned_chunk_data[i-1]["next_chunk_node_id"] = node_id
    return assigned_chunk_data

def send_chunk_to_node(node, chunk_data):
    url = node["url"] 
    print(chunk_data)
    if url != "http://localhost:5001":
        url = "http://localhost:5001"
    response = requests.post(url + '/chunk', data={
        'id': chunk_data['id'],
        'next_chunk_id': chunk_data.get("next_chunk_id") or "",
        'next_chunk_node_id': chunk_data.get('next_chunk_node_id') or "",
        'chunk': chunk_data['chunk']
    
    })
    return response.status_code==200

def get_chunk_from_node(node, chunk_id):
    url = node["url"]
    if url != "http://localhost:5001":
        url = "http://localhost:5001"
    print(chunk_id)
    response = requests.get(url + '/chunk/' + chunk_id)
    return response.json()

if __name__ == '__main__':
    app.run(debug=True,port=int(os.getenv('PORT', 5000)))  # Run the Flask app in debug mode
