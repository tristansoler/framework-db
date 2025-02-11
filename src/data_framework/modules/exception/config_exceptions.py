from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class ConfigError(DataFrameworkError):
    """Base class for all config module exceptions"""

    def __init__(self):
        super().__init__('Error initializing Data Framework config')


class ConfigFileNotFoundError(DataFrameworkError):
    """Error raised when a config file is not found in the specified path"""

    def __init__(self, config_file_path: str):
        super().__init__(f'Config file {config_file_path} not found')


class AccountNotFoundError(DataFrameworkError):
    """Error raised when a specific AWS accound ID is not found in the Data Framework config file"""

    def __init__(self, account_id: str, available_ids: List[str]):
        available_ids = ', '.join(available_ids)
        super().__init__(
            f'AWS account ID {account_id} not found in Data Framework config file. ' +
            f'Available account IDs: {available_ids}'
        )


class ParameterParseError(DataFrameworkError):
    """Error raised when the input parameters could not be parsed correctly"""

    def __init__(self, arguments: List[str]):
        super().__init__(f'Error parsing input arguments {arguments}')


class ConfigParseError(DataFrameworkError):
    """Error raised when the dataflow config could not be parsed correctly"""

    def __init__(self, field: str, field_type: str):
        super().__init__(
            f'Error parsing field \'{field}\' to type {field_type}. ' +
            'Please check this field in your config file'
        )


class EmptyProcessConfigError(DataFrameworkError):
    """Error raised when the configuration of a specific process is empty"""

    def __init__(self, process: str):
        super().__init__(
            f'Configuration of process {process} is empty'
        )


class ProcessNotFoundError(DataFrameworkError):
    """Error raised when a specific process is not found in the dataflow config file"""

    def __init__(self, process: str, available_processes: List[str]):
        available_processes = ', '.join(available_processes)
        super().__init__(
            f'Process {process} not found in config. Available processes: {available_processes}'
        )


class TableKeyError(DataFrameworkError):
    """Error raised when a specific table key is not found in the dataflow config file"""

    def __init__(self, table_key: str, available_table_keys: List[str]):
        available_table_keys = ', '.join(available_table_keys)
        super().__init__(
            f'Table key {table_key} not found in config file. Available table keys: {available_table_keys}'
        )


class TableConfigNotFoundError(DataFrameworkError):
    """Error raised when a specific database and table are not found in the dataflow config file"""

    def __init__(self, database: str, table: str):
        super().__init__(
            f'Table key for {database}.{table} not found in config file'
        )
