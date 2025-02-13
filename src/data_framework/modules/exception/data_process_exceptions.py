"""Definition of exceptions for data process module"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class TransformationNotImplementedError(DataFrameworkError):
    """Error raised when a transformation specified in the config file is not implemented yet"""

    def __init__(self, transformation: str, available_types: List[str] = None):
        error_message = f'Transformation type {transformation} not implemented'
        if available_types:
            available_types = ', '.join(available_types)
            error_message += f'. Available transformations: {available_types}'
        super().__init__(error_message)


class TransformationError(DataFrameworkError):
    """Error raised when a pre-defined transformation fails"""

    def __init__(self, transformation: str):
        super().__init__(f'Error performing {transformation} transformation')


class CastQueryError(DataFrameworkError):
    """Error raised when the generation of a query to cast data fails"""

    def __init__(self):
        super().__init__('Error generating query for data casting')


class CastDataError(DataFrameworkError):
    """Error raised when the casting of a table fails"""

    def __init__(
        self,
        source_database: str,
        source_table: str,
        target_database: str,
        target_table: str,
        casting_strategy: str
    ):
        super().__init__(
            f'Error casting data from table {source_database}.{source_table} ' +
            f'to table {target_database}.{target_table} with strategy {casting_strategy}'
        )


class ReadDataError(DataFrameworkError):
    """Error raised when a query to read data fails"""

    def __init__(self, query: str):
        super().__init__(f'Error reading data with query {query}')


class WriteDataError(DataFrameworkError):
    """Error raised when data could not be written successfully in a table"""

    def __init__(self, database: str, table: str):
        super().__init__(f'Error writing data to {database}.{table}')


class DeleteDataError(DataFrameworkError):
    """Error raised when data could not be deleted successfully from a table"""

    def __init__(self, database: str, table: str):
        super().__init__(f'Error deleting data from {database}.{table}')


class DataProcessError(DataFrameworkError):
    """Error raised when a data transformation fails"""

    def __init__(self, error_message: str = 'Error performing data transformation'):
        super().__init__(error_message)


class SparkConfigurationError(DataFrameworkError):
    """Error raised when Spark configuration fails"""

    def __init__(self, error_message: str = 'Error configuring Spark session'):
        super().__init__(error_message)
