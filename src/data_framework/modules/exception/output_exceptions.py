from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class OutputError(DataFrameworkError):
    """Error raised when one or more output files could not be generated correctly"""

    def __init__(self, failed_outputs: List[str]):
        failed_outputs = ', '.join(failed_outputs)
        super().__init__(
            f'Error generating the following outputs: {failed_outputs}. ' +
            'Check logs for more information'
        )


class OutputGenerationError(DataFrameworkError):
    """Error raised when a specific output file could not be generated correctly"""

    def __init__(self, output_name: str, error_message: str):
        super().__init__(
            f'Error generating output {output_name}: {error_message}'
        )


class NoDataError(DataFrameworkError):
    """Error raised when there is no data to include in a specific output file"""

    def __init__(self, output_name: str):
        super().__init__(
            f'No data available for output {output_name}'
        )
