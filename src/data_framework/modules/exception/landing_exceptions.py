"""Definition of exceptions for landing step"""

from data_framework.modules.exception.generic_exceptions import DataFrameworkError


class FileProcessError(DataFrameworkError):
    """Error raised when a file could not be processed correctly"""

    def __init__(self, file_path: str):
        super().__init__(f'Error processing file {file_path}')


class FileReadError(DataFrameworkError):
    """Error raised when a file could not be read correctly"""

    def __init__(self, file_path: str):
        super().__init__(f'Error reading file {file_path}')


class InvalidDateRegexError(DataFrameworkError):
    """Error raised when the date in a filename does not match the specified regex"""

    def __init__(self, filename: str, pattern: str):
        super().__init__(
            f'The date in the filename {filename} does not match the pattern {pattern}. ' +
            'Please check the date regex pattern in your config file'
        )


class InvalidRegexGroupError(DataFrameworkError):
    """Error raised when the groups in the filename date regex are invalid"""

    def __init__(self, pattern: str):
        super().__init__(
            f'The name of the groups in the date regex pattern {pattern} must be year, month and day. ' +
            'Example: (?P<year>\\d{4})_(?P<month>\\d{2})_(?P<day>\\d{2}). ' +
            'Please check the date regex pattern in your config file'
        )


class InvalidFileError(DataFrameworkError):
    """Error raised when a file does not meet the defined quality controls"""

    def __init__(self, file_path: str):
        super().__init__(
            f'The file {file_path} has failed quality controls. ' +
            'Check controls results table for more information'
        )
