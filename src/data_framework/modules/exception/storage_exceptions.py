"""Definition of exceptions for storage module"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class StorageError(DataFrameworkError):
    """Error raised when a storage generic operation fails"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class StorageReadError(DataFrameworkError):
    """Error raised when a storage read operation fails"""

    def __init__(self, path: str):
        super().__init__(f'Error reading file {path}')


class StorageWriteError(DataFrameworkError):
    """Error raised when a storage write operation fails"""

    def __init__(self, path: str):
        super().__init__(f'Error writing file to {path}')
