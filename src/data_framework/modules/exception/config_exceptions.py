from data_framework.modules.exception.generic_exceptions import DataFrameworkError
from typing import List


class ConfigError(DataFrameworkError):

    def __init__(self):
        super().__init__('Error initializing Data Framework config')


class ConfigFileNotFoundError(DataFrameworkError):

    def __init__(self, config_file_path: str):
        super().__init__(f'Config file {config_file_path} not found')


class AccountNotFoundError(DataFrameworkError):

    def __init__(self, account_id: str, available_ids: List[str]):
        available_ids = ', '.join(available_ids)
        super().__init__(
            f'AWS account ID {account_id} not found in Data Framework config file. ' +
            f'Available account IDs: {available_ids}'
        )


class ParameterParseError(DataFrameworkError):

    def __init__(self, arguments: List[str]):
        super().__init__(f'Error parsing input arguments {arguments}')


class ConfigParseError(DataFrameworkError):

    def __init__(self, field: str, field_type: str):
        super().__init__(
            f'Error parsing field \'{field}\' to type {field_type}. ' +
            'Please check this field in your transformation.json file'
        )


class TransformationConfigError(DataFrameworkError):

    def __init__(self, config_file_path: str):
        super().__init__(
            f'Error reading transformation config file {config_file_path}'
        )
