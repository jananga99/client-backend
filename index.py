import io
from flask import Flask, request, jsonify, send_file
import services.file_service as file_service
import os
import dotenv
from exceptions.error import Error
from validators.validators import validate_access_type
from classes.access_type import AccessType

dotenv.load_dotenv()

app = Flask(__name__)


@app.route("/file", methods=["POST"])
def upload_file():
    try:
        file = request.files["file"]
        access_type = request.form["accessType"]
        metadata = file_service.upload_file(file, access_type)
        return jsonify(metadata)
    except Error as e:
        print("Error : ", e.message)
        return jsonify({"message": e.message}), e.status_code
    except Exception as e:
        print(e)
        print("An unexpected error occured")
        return jsonify({"message": "Unexpected error occured"}), 500


@app.route("/file/<file_id>", methods=["GET"])
def get_file(file_id):
    try:
        metadata, combined_file = file_service.get_file(file_id)
        data_stream = io.BytesIO(combined_file)
        return send_file(
            data_stream,
            as_attachment=True,
            download_name=metadata["name"],
            mimetype=metadata["type"],
        )
    except Error as e:
        print("Error : ", e.message)
        return jsonify({"message": e.message}), e.status_code
    except Exception as e:
        print(e)
        print("An unexpected error occured")
        return jsonify({"message": "Unexpected error occured"}), 500


@app.route("/file", methods=["GET"])
def get_all_metadata():
    try:
        access_type = request.args.get("accessType")
        search = request.args.get("search")
        if access_type is not None:
            validate_access_type(access_type)
            if access_type == AccessType.PUBLIC.value:
                metadata = file_service.get_all_public_metadata(search=search)
            elif access_type == AccessType.PRIVATE.value:
                metadata = file_service.get_all_private_metadata(search=search)
            else:
                raise Error("Invalid access type", 500)
        else:
            metadata = (
                file_service.get_all_public_metadata(search=search)
                + file_service.get_all_private_metadata(search=search)
            )
        return jsonify(metadata)
    except Error as e:
        print("Error : ", e.message)
        return jsonify({"message": e.message}), e.status_code
    except Exception as e:
        print(e)
        print("An unexpected error occured")
        return jsonify({"message": "Unexpected error occured"}), 500


@app.route("/file-test", methods=["POST"])
def test_file():
    try:
        file = request.files["file"]
        combined_file = file_service.test_split_combine_file(file)
        data_stream = io.BytesIO(combined_file)
        return send_file(data_stream, as_attachment=True, download_name=file.filename)
    except Error as e:
        print("Error : ", e.message)
        return jsonify({"message": e.message}), e.status_code
    except Exception as e:
        print(e)
        print("An unexpected error occured")
        return jsonify({"message": "Unexpected error occured"}), 500


# Delete route
@app.route("/file/<file_id>", methods=["DELETE"])
def delete_file(file_id):
    try:
        file_service.delete_file(file_id)
        print("File deleted in delete_file")
        return jsonify({"message": "File deleted successfully"})
    except Error as e:
        print("Error : ", e.message)
        return jsonify({"message": e.message}), e.status_code
    except Exception as e:
        print(e)
        print("An unexpected error occured")
        return jsonify({"message": "Unexpected error occured"}), 500


if __name__ == "__main__":
    app.run(debug=True, port=int(os.getenv("PORT", 5000)))
