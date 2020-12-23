from flask import jsonify, Flask, request
import os
from normalization import Normalize
from utils import Database, UserRequest, Metadata
from constants import *

app = Flask(__name__)


@app.route("/normalization", methods=["POST"])
def create_normalization() -> jsonify:
    database_url = os.environ[DATABASE_URL]
    database_replica_set = os.environ[DATABASE_REPLICA_SET]
    database_name = os.environ[DATABASE_NAME]
    parent_filename = request.json[PARENT_FILENAME_NAME]
    normalization_filename = request.json[NORMALIZATION_FILENAME_NAME]
    normalization_field = request.json[FIELDS_NAME]

    database = Database(
        database_url,
        database_replica_set,
        os.environ[DATABASE_PORT],
        database_name,
    )

    request_validator = UserRequest(database)

    request_errors = analyse_request_errors(
        request_validator,
        parent_filename,
        normalization_filename,
        normalization_field)

    if request_errors is not None:
        return request_errors

    database_url_input = Database.collection_database_url(
        database_url,
        database_name,
        parent_filename,
        database_replica_set,
    )

    database_url_output = Database.collection_database_url(
        database_url,
        database_name,
        normalization_filename,
        database_replica_set,
    )

    metadata_creator = Metadata(database)
    normalization = Normalize(database_url_input,
                              database_url_output,
                              metadata_creator)

    normalization.create()

    return (
        jsonify({
            MESSAGE_RESULT:
                MICROSERVICE_URI_GET +
                normalization_filename +
                MICROSERVICE_URI_GET_PARAMS}),
        HTTP_STATUS_CODE_SUCCESS_CREATED,
    )


def analyse_request_errors(request_validator: UserRequest,
                           parent_filename: str,
                           normalization_filename: str,
                           field_name: str) -> jsonify:
    try:
        request_validator.parent_filename_validator(
            parent_filename
        )
    except Exception as invalid_filename:
        return (
            jsonify({MESSAGE_RESULT: invalid_filename.args[
                FIRST_ARGUMENT]}),
            HTTP_STATUS_CODE_CONFLICT,
        )

    try:
        request_validator.normalization_filename_validator(
            normalization_filename)
    except Exception as invalid_normalization_filename:
        return (
            jsonify({MESSAGE_RESULT: invalid_normalization_filename.args[
                    FIRST_ARGUMENT]}),
            HTTP_STATUS_CODE_NOT_ACCEPTABLE,
        )

    try:
        request_validator.filename_field_validator(
            parent_filename, field_name
        )
    except Exception as invalid_field:
        return (
            jsonify({MESSAGE_RESULT: invalid_field.args[FIRST_ARGUMENT]}),
            HTTP_STATUS_CODE_NOT_ACCEPTABLE,
        )

    return None


if __name__ == "__main__":
    app.run(host=os.environ[DATA_TYPE_HANDLER_HOST],
            port=int(os.environ[DATA_TYPE_HANDLER_PORT]))
