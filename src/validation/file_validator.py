"""
File validator
--------------

Requirements:
    - python==3.8.0
    - pandas==1.5.3
"""

import os
import re
import json
import zipfile
from pathlib import Path
from io import BytesIO
import pandas as pd
from pandas import DataFrame
from src.dataplatform_tools.logger import configure_logger


class FileValidator(object):

    def __init__(self, file_key: str, data_flow: str, source_bucket: str, athena_bucket: str, config_file: str) -> None:
        self.file_key = file_key
        self.source_bucket = source_bucket
        self.athena_bucket = athena_bucket
        self.data_flow = data_flow
        self.catalog = 'iceberg_catalog'
        self.common_db = 'rl_funds_common'
        self.controls_table = 'quality_controls'
        self.raw_table = ''
        self.s3_client = None
        self.logger = configure_logger('File Validator', 'INFO')
        # File features
        self.config = self.get_file_config(config_file)
        self.file_extension = self.get_file_extension(file_key)
        self.full_filename = self.get_filename(file_key, with_extension=True)
        self.short_filename = self.get_filename(file_key, with_extension=False)
        self.file_contents = self.get_file_contents(file_key)

    def get_file_config(self, config_file: str) -> dict:
        # TODO: obtener fichero config de S3
        if os.path.exists(config_file):
            self.logger.info(f'Reading config file {config_file}')
            with open(config_file, 'r') as f:
                config = json.load(f)
            if not config.get(self.data_flow):
                raise FileNotFoundError(f'Dataflow {self.data_flow} not found in config file {config_file}')
            return config[self.data_flow]
        else:
            raise FileNotFoundError(f'Config file {config_file} not found')

    def get_file_extension(self, file_key: str) -> str:
        try:
            return Path(file_key).suffix.strip('.').lower()
        except Exception as e:
            self.logger.error(f'Extension of the file {file_key} could not be obtained. Error: {e}')
            return ''

    def get_filename(self, file_key: str, with_extension: bool) -> str:
        try:
            if with_extension:
                return Path(file_key).name
            else:
                return Path(file_key).name.split('.')[0]
        except Exception as e:
            self.logger.error(f'Name of the file {file_key} could not be obtained. Error: {e}')
            return ''

    def get_file_contents(self, file_key: str) -> dict:
        # TODO: obtain from S3
        try:
            file_contents = {}
            if self.file_extension == 'zip':
                # TODO: otros tipos de archivos comprimidos
                with zipfile.ZipFile(file_key, 'r') as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            file_contents[filename] = BytesIO(f.read())
            else:
                with open(file_key, 'rb') as f:
                    file_contents[self.full_filename] = BytesIO(f.read())
            return file_contents
        except Exception as e:
            self.logger.error(f'Contents of the file {file_key} could not be obtained. Error: {e}')
            return {}

    def validate_file(self) -> bool:
        valid_extension = self.validate_extension()
        valid_filename = self.validate_filename()
        valid_csv = self.validate_csv_files()
        # TODO: agregar resultados + registrar controles

    def validate_extension(self) -> bool:
        self.logger.info(f'Validating extension of file {self.full_filename}')
        if self.config['extension'] == self.file_extension:
            if self.config['compressed']:
                # Validate extension of the files inside the compressed file
                filenames = self.file_contents.keys()
                expected_extension = self.config['uncompressed_extension']
                for filename in filenames:
                    # Assumes that all the files inside the compressed file have the same extension
                    extension = self.get_file_extension(filename)
                    if expected_extension != extension:
                        return False
                return True
            else:
                return True
        else:
            return False

    def validate_filename(self) -> bool:
        self.logger.info(f'Validating name of file {self.full_filename}')
        main_pattern = self.convert_to_regex(self.config['filename_pattern'])
        if bool(re.match(main_pattern, self.short_filename)):
            if self.config['compressed']:
                filenames = self.file_contents.keys()
                pattern = self.convert_to_regex(self.config['uncompressed_filename_pattern'])
                for filename in filenames:
                    # Assumes that all the files inside the compressed file have the same pattern
                    if not bool(re.match(pattern, filename)):
                        return False
            else:
                return True
        else:
            return False

    def validate_csv_files(self) -> bool:
        for filename, content in self.file_contents.items():
            if self.get_file_extension(filename) == 'csv':
                self.logger.info(f'Validating {filename} contents')
                # TODO: validate number of columns and rows
                try:
                    df = pd.read_csv(
                        content,
                        delimiter=self.config['delimiter'],
                        header=self.config['header_line']
                    )
                except Exception as e:
                    self.logger.error(f'Error validating {filename} contents: {e}')
                    return False
                else:
                    if not self.validate_columns(df):
                        return False
        return True

    def validate_columns(self, df: DataFrame) -> bool:
        columns = list(df.columns)
        # TODO: obtain columns from raw table
        expected_columns = self.config['columns']
        if self.config['ordered_columns']:
            if columns != expected_columns:
                self.logger.error(f'Column names do not match. Expected: {expected_columns}. Found: {columns}')
                return False
        else:
            extra_columns = list(set(columns) - set(expected_columns))
            missing_columns = list(set(expected_columns) - set(columns))
            diff_columns = extra_columns + missing_columns
            if len(diff_columns) > 0:
                self.logger.error(
                    f'Column names do not match. Missing columns: {missing_columns}. Extra columns: {extra_columns}'
                )
                return False
        self.logger.info('All column names are valid')
        return True

    @staticmethod
    def convert_to_regex(expression: str) -> str:
        pattern = expression \
            .replace('YYYY', r'\d{4}') \
            .replace('MM', r'\d{2}') \
            .replace('DD', r'\d{2}')
        return pattern


if __name__ == '__main__':
    file_validator = FileValidator(
        'C:/Users/x312756/Documents/Projects/1. Scripts/file_validator/files/file_package_2024_09_01.zip',
        'dataflow_with_compressed_file',
        '',
        '',
        'src/validation/config.json'
    )
    file_validator.validate_file()

    file_validator = FileValidator(
        'C:/Users/x312756/Documents/Projects/1. Scripts/file_validator/files/dummy_filename_2024_09_01.csv',
        'dataflow_with_uncompressed_file',
        '',
        '',
        'src/validation/config.json'
    )
    file_validator.validate_file()
