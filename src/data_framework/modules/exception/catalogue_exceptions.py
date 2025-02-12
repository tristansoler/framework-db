from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class CreatePartitionError(DataFrameworkError):
    """Error raised when a partition could not be created in a table"""

    def __init__(
        self,
        database: str,
        table: str,
        partition_field: str,
        partition_value: str
    ):
        super().__init__(
            f'Error creating partition {partition_field}={partition_value} in {database}.{table}'
        )


class InvalidPartitionFieldError(DataFrameworkError):
    """Error raised when the specified partition field does not exist in a table"""

    def __init__(
        self,
        database: str,
        table: str,
        partition_field: str
    ):
        super().__init__(
            f'Field {partition_field} does not exist in {database}.{table}'
        )


class SchemaError(DataFrameworkError):
    """Error raised when a table schema could not be obtained correctly"""

    def __init__(self, database: str, table: str):
        super().__init__(
            f'Error obtaining schema of {database}.{table}'
        )
