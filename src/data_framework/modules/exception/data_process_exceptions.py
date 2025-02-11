from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class TransformationNotImplementedError(DataFrameworkError):
    """Error raised when a transformation specified in the config file is not implemented yet"""

    def __init__(self, transformation: str, available_types: List[str]):
        available_types = ', '.join(available_types)
        super().__init__(
            f'Transformation type {transformation} not implemented. Available transformations: {available_types}'
        )
