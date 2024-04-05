import io
from bson import ObjectId
from flask import Flask, request, jsonify, send_file
import services.file_service as file_service
import os
import dotenv

dotenv.load_dotenv()

app = Flask(__name__)


@app.route("/file", methods=["POST"])
def upload_file():
    try:
        file = request.files["file"]
        access_type = request.form["accessType"]
        metadata = file_service.upload_file(file, access_type)
        return jsonify(metadata)
    except Exception as e:
        print("An error occured")
        return jsonify({"message": "Unexpected error occured"})


@app.route("/file/<file_id>", methods=["GET"])
def get_file(file_id):
    try:
        file_id = ObjectId(file_id)
        metadata, combined_file = file_service.get_file(file_id)
        data_stream = io.BytesIO(combined_file)
        return send_file(
            data_stream,
            as_attachment=True,
            download_name=metadata["name"],
            mimetype=metadata["type"],
        )
    except Exception as e:
        print("An error occured")
        return jsonify({"message": "Unexpected error occured"})


@app.route("/file", methods=["GET"])
def get_all_metadata():
    try:
        all_metadata = file_service.get_all_metadata()
        return jsonify(all_metadata)
    except Exception as e:
        print("An error occured")
        return jsonify({"message": "Unexpected error occured"})


if __name__ == "__main__":
    app.run(
        debug=True, port=int(os.getenv("PORT", 5000))
    )  