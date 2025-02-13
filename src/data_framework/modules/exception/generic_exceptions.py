"""Definition of generic data framework exceptions"""

import traceback


class DataFrameworkError(Exception):
    """Base class for all Data Framework exceptions"""

    def format_exception(self) -> str:
        error_type = type(self).__name__
        error_message = str(self)
        error_trace = traceback.format_exc()
        return f'Error type: {error_type}\n\nDescription: {error_message}\n\nTrace:\n\n{error_trace}'


class LoggerInitializationError(DataFrameworkError):
    """Error raised when logger could not be configured correctly"""

    def __init__(self):
        super().__init__('Failed to initialize logger')
