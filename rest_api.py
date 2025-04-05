from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.json_util import dumps
from bson import ObjectId

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["scraping_data"]
collection = db["scraped_items"]

# Helper function to serialize MongoDB ObjectId
def serialize_mongo_document(doc):
    doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
    return doc

@app.route('/api/data', methods=['GET', 'POST'])
def receive_data():
    if request.method == 'POST':
        try:
            # Parse JSON data from the request
            data = request.get_json()
            if data:
                # Insert data into MongoDB
                result = collection.insert_one(data)
                return jsonify({
                    "message": "Data received and stored successfully",
                    "inserted_id": str(result.inserted_id)
                }), 200
            else:
                return jsonify({"error": "No data received"}), 400
        except Exception as e:
            return jsonify({"error": "Invalid JSON", "details": str(e)}), 400

    elif request.method == 'GET':
        # Retrieve all data from MongoDB
        data = collection.find()  # Fetch all documents
        data_list = [serialize_mongo_document(doc) for doc in data]  # Serialize ObjectId
        return jsonify({"message": "Data retrieved successfully", "data": data_list}), 200

if __name__ == '__main__':

    app.run(debug=True)
