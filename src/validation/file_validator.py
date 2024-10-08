"""
File validator
--------------

Example usage:
    from src.validation.file_validator import FileValidator
    from src.dataplatform_tools.logger import configure_logger
    logger = configure_logger('MyAppName', 'INFO')
    file_validator = FileValidator(
        logger,
        'path/to/file/my_file.csv',
        'dataflow_name',
        'source_bucket',
        'athena_bucket',
        'config_bucket',
        'path/to/config/config.json'
    )
    file_validator.validate_file()
"""

import re
import zipfile
from logging import Logger
from pathlib import Path
from io import BytesIO
import pandas as pd
from pandas import DataFrame
from src.dataplatform_tools.logger import configure_logger
from src.dataplatform_tools.s3 import S3Client
from src.dataplatform_tools.config import read_json_config_from_s3
from src.dataplatform_tools.glue import GlueClientTool


class FileValidator(object):

    def __init__(
        self,
        logger: Logger,
        file_key: str,
        data_flow: str,
        source_bucket: str,
        athena_bucket: str,
        config_bucket: str,
        config_key: str
    ) -> None:
        # Input arguments
        self.file_key = file_key
        self.data_flow = data_flow
        self.config_key = config_key
        self.source_bucket = source_bucket
        self.athena_bucket = athena_bucket
        self.config_bucket = config_bucket
        # Logging
        self.logger = logger
        # AWS
        self.s3_client = S3Client(logger)
        self.glue_client = GlueClientTool()
        # self.catalog = 'iceberg_catalog'
        # self.common_db = 'rl_funds_common'
        # self.controls_table = 'quality_controls'
        # File features
        self.config = self.get_file_config()
        self.file_extension = self.get_file_extension(file_key)
        self.full_filename = self.get_file_name(file_key, with_extension=True)
        self.short_filename = self.get_file_name(file_key, with_extension=False)
        self.file_contents = self.get_input_file_contents()

    def get_file_config(self) -> dict:
        try:
            self.logger.info(
                f'Reading config file {self.config_key} from bucket {self.config_bucket}'
            )
            config = read_json_config_from_s3(self.s3_client, self.config_bucket, self.config_key)
            return config
        except Exception as e:
            self.logger.error(f'Error reading config file: {e}')
            return {}

    def get_file_extension(self, file_key: str) -> str:
        try:
            return Path(file_key).suffix.strip('.').lower()
        except Exception as e:
            self.logger.error(f'Extension of the file {file_key} could not be obtained. Error: {e}')
            return ''

    def get_file_name(self, file_key: str, with_extension: bool) -> str:
        try:
            if with_extension:
                return Path(file_key).name
            else:
                return Path(file_key).name.split('.')[0]
        except Exception as e:
            self.logger.error(f'Name of the file {file_key} could not be obtained. Error: {e}')
            return ''

    def get_input_file_contents(self) -> dict:
        try:
            s3_file_content = self.s3_client.get_file_content_from_s3(
                self.source_bucket, self.file_key
            )
            file_contents = {}
            if self.file_extension == 'zip':
                with zipfile.ZipFile(s3_file_content, 'r') as z:
                    for filename in z.namelist():
                        with z.open(filename) as f:
                            file_contents[filename] = BytesIO(f.read())
            else:
                file_contents[self.full_filename] = s3_file_content
            return file_contents
        except Exception as e:
            self.logger.error(f'Contents of the file {self.file_key} could not be obtained. Error: {e}')
            return {}

    def validate_file(self) -> bool:
        valid_extension = self.validate_extension()
        valid_filename = self.validate_filename()
        valid_csv = self.validate_csv_files()
        # TODO: agregar resultados + registrar controles + mover ficheros

    def validate_extension(self) -> bool:
        self.logger.info(f'Validating extension of file {self.full_filename}')
        if self.config['source']['extension'] == self.file_extension:
            if self.config['source']['compressed']:
                # Validate extension of the files inside the compressed file
                filenames = self.file_contents.keys()
                expected_extension = self.config['source']['uncompressed_extension']
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
        main_pattern = self.convert_to_regex(self.config['source']['filename_pattern'])
        if bool(re.match(main_pattern, self.short_filename)):
            if self.config['source']['compressed']:
                filenames = self.file_contents.keys()
                pattern = self.convert_to_regex(self.config['source']['uncompressed_filename_pattern'])
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
                try:
                    df = pd.read_csv(
                        content,
                        delimiter=self.config['source']['delimiter'],
                        header=self.config['source']['header_line']
                    )
                    expected_n_rows = content.getvalue().count(b'\n') - self.config['source']['header_line']
                    expected_n_columns = self.get_expected_number_of_columns(content)
                    if df.shape != (expected_n_rows, expected_n_columns):
                        self.logger.error(f'Invalid delimiter and/or header line for {filename}')
                        return False
                    if not self.validate_columns(df):
                        return False
                except Exception as e:
                    self.logger.error(f'Error validating {filename} contents: {e}')
                    return False
        return True

    def validate_columns(self, df: DataFrame) -> bool:
        columns = list(df.columns)
        expected_columns = self.get_expected_columns()
        if self.config['source']['ordered_columns']:
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

    def get_expected_columns(self) -> list:
        try:
            db_name = self.config['target']['db_rl']
            columns = self.glue_client.get_table_columns(db_name, self.data_flow)
            return columns
        except Exception as e:
            self.logger.error(f'Error obtaining expected columns from Glue: {e}')
            return []

    def get_expected_number_of_columns(self, csv_content: BytesIO) -> int:
        csv_content.seek(0)
        for i, line in enumerate(csv_content):
            if i == self.config['source']['header_line']:
                return len(line.decode('utf-8').split(';'))

    @staticmethod
    def convert_to_regex(expression: str) -> str:
        pattern = expression \
            .replace('YYYY', r'\d{4}') \
            .replace('MM', r'\d{2}') \
            .replace('DD', r'\d{2}')
        return pattern


if __name__ == '__main__':
    logger = configure_logger('File Validator', 'INFO')
    file_validator = FileValidator(
        logger,
        'factset_plcartera/inbound/file_package_2024_09_01.zip',
        'dataflow_compressed_file',
        'aihd1airas3aihgdp-landing',
        '',
        'aihd1airas3aihgdp-landing',
        'factset_plcartera/inbound/config_compressed_file.json'
    )
    file_validator.validate_file()

    file_validator = FileValidator(
        logger,
        'factset_plcartera/inbound/dummy_filename_2024_09_01.csv',
        'dataflow_uncompressed_file',
        'aihd1airas3aihgdp-landing',
        '',
        'aihd1airas3aihgdp-landing',
        'factset_plcartera/inbound/config_uncompressed_file.json'
    )
    file_validator.validate_file()
