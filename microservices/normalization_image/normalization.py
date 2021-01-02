import numpy
from sklearn.preprocessing import normalize

from utils import Metadata, Database
from concurrent.futures import ThreadPoolExecutor
from constants import *


class Normalization:

    def __init__(self, database: Database, metadata: Metadata):
        self.__metadata = metadata
        self.__database = database
        self.__thread_pool = ThreadPoolExecutor()

    def create(self, parent_filename: str,
               normalization_filename: str,
               field: str,
               normalization_type: str,
               function_parameters: dict):

        self.__metadata.create_file(normalization_filename)
        self.__thread_pool.submit(self.__pipeline, parent_filename,
                                  normalization_filename, field,
                                  normalization_type, function_parameters)

    def __pipeline(self, parent_filename: str,
                   normalization_filename: str,
                   field: str,
                   normalization_type: str,
                   function_parameters: dict):
        field_value_list = []
        id_list = []

        query = [
            {},
            {
             field: FIELD_VALUE
            }
        ]
        filtered_collection = self.__database.find(parent_filename, query)

        for item in filtered_collection:
            if item != {DOCUMENT_ID_NAME: item[DOCUMENT_ID_NAME]} \
                    and item[field] is not None:
                field_value_list.append(item[field])
                id_list.append(item[DOCUMENT_ID_NAME])

        value_matrix = [numpy.array(field_value_list)]
        normalized_value_matrix = normalize(value_matrix,
                                            norm=normalization_type,
                                            *function_parameters)

        for index in range(len(id_list)):
            new_field_value = normalized_value_matrix[MATRIX_ROW_INDEX][index]
            document = {
                DOCUMENT_ID_NAME: id_list[index],
                field: new_field_value
            }

            self.__database.insert_one_in_file(normalization_filename, document)
