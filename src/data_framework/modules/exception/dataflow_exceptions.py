from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class DataflowInitializationError(DataFrameworkError):
    """Error raised when dataflow class could not be initialized correctly"""

    def __init__(self):
        super().__init__('Error initializing dataflow process')


class PayloadResponseError(DataFrameworkError):
    """Error raised when dataflow response could not be generated correctly"""

    def __init__(self):
        super().__init__('Error generating dataflow payload response')
