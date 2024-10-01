"""
File validator
--------------

Requirements:
    - python==3.8.0
    - pandas==1.5.3
"""

import sys
import os
import re
import logging
import json
import zipfile
import pandas as pd
from pandas import DataFrame

# Logger
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()
handler = logging.FileHandler('file_validator.log', 'w', 'utf-8')
handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
logger.addHandler(handler)


def get_file_config(event: dict) -> dict:
    if os.path.exists(event['config_file']):
        with open(event['config_file'], 'r') as f:
            config = json.load(f)
        for file_config in config:
            if (
                file_config['dataflow_name'] == event['dataflow'] and
                validate_filename(event['key'], file_config['main_filename_pattern']) and
                validate_extension(event['key'], file_config['main_extension'])
            ):
                return file_config
        raise FileNotFoundError(f"Format info not found for {event['key']}")
    else:
        raise FileNotFoundError(f"Config file {event['config_file']} not found")


def get_file_extension(filename: str) -> str:
    if '.' in filename:
        return filename.split('.')[-1].lower()
    else:
        raise ValueError(f'File {filename} does not have an extension')


def validate_filename(filename: str, filename_pattern: str) -> bool:
    if '/' in filename:
        filename = filename.split('/')[-1]
    if '.' in filename:
        filename = filename.split('.')[0]
    filename_pattern = filename_pattern \
        .replace('YYYY', r'\d{4}') \
        .replace('MM', r'\d{2}') \
        .replace('DD', r'\d{2}')
    result = bool(re.match(filename_pattern, filename))
    return result


def validate_extension(filename: str, expected_extension: str) -> str:
    file_extension = get_file_extension(filename)
    result = file_extension == expected_extension
    if not result:
        raise ValueError(f'File {filename} does not have the expected extension {expected_extension}')
    return result


def read_compressed_file_contents(zip_filename: str, file_config: dict) -> bytes:
    if file_config['main_extension'] == 'zip':
        with zipfile.ZipFile(zip_filename, 'r') as z:
            for filename in z.namelist():
                if (
                    validate_filename(filename, file_config['filename_pattern']) and
                    validate_extension(filename, file_config['extension'])
                ):
                    file_path = z.extract(filename, path='files')
                    return file_path
    # TODO: otros tipos de archivos comprimidos
    raise FileNotFoundError(f"File not found in compressed archive: {zip_filename}")


def validate_file(input_file: str, file_config: dict) -> bool:
    logger.info(f"Validating {input_file}")
    extension = get_file_extension(input_file)
    if extension == 'csv':
        df = pd.read_csv(
            input_file,
            delimiter=file_config['delimiter'],
            header=file_config['header_line']
        )
        logger.info('CSV separator and header are valid')
        validate_column_names(df, file_config)
    # TODO: otros tipos de archivos


def validate_column_names(df: DataFrame, file_config: dict) -> None:
    columns = df.columns.tolist()
    expected_columns = file_config['columns']
    if file_config['ordered_columns']:
        if columns != expected_columns:
            raise ValueError(f'Column names do not match. Expected: {expected_columns}. Found: {columns}')
        else:
            logger.info('All column names are valid')
    else:
        extra_columns = list(set(columns) - set(expected_columns))
        missing_columns = list(set(expected_columns) - set(columns))
        diff_columns = extra_columns + missing_columns
        if len(diff_columns) > 0:
            raise ValueError(
                f'Column names do not match. Missing columns: {missing_columns}. Extra columns: {extra_columns}'
            )
        else:
            logger.info('All column names are valid')


def handler(event: dict, context: str) -> None:
    try:
        # Load format info for the input file
        logger.info(f"Obtaining config for {event['key']}")
        file_config = get_file_config(event)

        # Uncompress input file if necessary
        if file_config['compressed']:
            logger.info(f'Decompressing {event["key"]}')
            input_file = read_compressed_file_contents(event['key'], file_config)
        else:
            input_file = event['key']

        # Read and validate input file
        validate_file(input_file, file_config)
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    event = {
        'key': 'files/file_package_2024_09_01.zip',
        'dataflow': 'dummy_dataflow',
        'config_file': 'config.json'
    }
    handler(event, 'dummy_context')
