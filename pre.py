from pymongo import MongoClient

MONGODB_URI = 'mongodb+srv://abcd:abcd@cluster0.yrys0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
client = MongoClient(MONGODB_URI)
local_db = client['local-db']
local_nodes = local_db['nodes']

def add_nodes():
    for i in range(5):
        local_nodes.insert_one({
            "_id":i+1,
            "address": f"Container {i+1}",
        })

add_nodes()