class Normalize:
    METADATA_DOCUMENT_ID = 0
    DOCUMENT_ID_NAME = "_id"
    STRING_TYPE = "string"
    NUMBER_TYPE = "number"

    def __init__(self, database_url_input: str,
                 database_url_output: str,
                 metadata_handler: Metadata):
        self.database_input = database_url_input
        self.database_output = database_url_output
        self.metadata_handler = metadata_handler

    def create(self):
        return
