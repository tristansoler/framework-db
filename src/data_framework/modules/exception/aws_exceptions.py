"""Definition of exceptions for AWS services"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class STSError(DataFrameworkError):

    def __init__(self, error_message: str):
        super().__init__(error_message)


class GlueError(DataFrameworkError):

    def __init__(self, error_message: str):
        super().__init__(error_message)


class AthenaError(DataFrameworkError):

    def __init__(self, error_message: str):
        super().__init__(error_message)


class SSMError(DataFrameworkError):

    def __init__(self, error_message: str):
        super().__init__(error_message)
