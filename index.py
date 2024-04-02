from datetime import datetime
from flask import Flask, request, jsonify
from pymongo import MongoClient
from enum import Enum

app = Flask(__name__)

# Connect to MongoDB
MONGODB_URI = 'mongodb+srv://abcd:abcd@cluster0.yrys0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
client = MongoClient(MONGODB_URI)
global_db = client['global']
global_metadata = global_db['metadata']

local_db = client['local-db']
local_metadata = local_db['metadata']


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
    print("Fle update req received")
    file = request.files['file']
    access_type = request.form['accessType']
    print(file.filename)
    print(file.content_type)
    print(file.content_length)
    print(access_type)
    # Split to chunks
    merkel_root = "Dummy Merkel Root"
    # Randomly assign to nodes
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

    
    # Send to nodes


    return jsonify({'success': True, 'process': 'File uploaded successfully.'})

def add_to_global_db(metadata):
    result = global_metadata.insert_one(metadata)
    return result.inserted_id

def add_to_local_db(metadata):
    result = local_metadata.insert_one(metadata)
    return result.inserted_id

if __name__ == '__main__':
    app.run(debug=True)  # Run the Flask app in debug mode
