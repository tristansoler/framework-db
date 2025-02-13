"""Definition of exceptions for AWS services"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class STSError(DataFrameworkError):
    """Error related to STS"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class GlueError(DataFrameworkError):
    """Error related to Glue"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class AthenaError(DataFrameworkError):
    """Error related to Athena"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class SSMError(DataFrameworkError):
    """Error related to SSM"""

    def __init__(self, error_message: str):
        super().__init__(error_message)


class S3Error(DataFrameworkError):
    """Error related to S3"""

    def __init__(self, error_message: str):
        super().__init__(error_message)
