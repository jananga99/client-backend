from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# Connect to MongoDB
MONGODB_URI = 'mongodb://atlas-sql-62cc3f34462aa55873a0c10f-yrys0.a.query.mongodb.net/distributed?ssl=true&authSource=admin'
client = MongoClient(MONGODB_URI)
db = client['distributed']
collection = db['global-db']

if __name__ == '__main__':
    app.run(debug=True)  # Run the Flask app in debug mode
