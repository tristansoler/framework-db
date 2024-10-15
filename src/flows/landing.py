import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import pandas as pd
from pandas import DataFrame
from modules.config.core import config
from modules.utils.logger import logger
from modules.storage.core_storage import Storage
from modules.catalogue.core_catalogue import CoreCatalogue


class ProcessingCoordinator:

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.storage = Storage._storage
        self.catalogue = CoreCatalogue._catalogue
        self.incoming_file_config = self.config.flows.landing_to_raw.incoming_file
        self.output_file_config = self.config.flows.landing_to_raw.output_file
        self.validations = self.config.flows.landing_to_raw.incoming_file.validations

    def process(self):
        # Read file from S3
        file_contents = self.read_data()

        # Apply controls
        is_valid = self.validate_file(file_contents)

        if is_valid:
            # Save uncompressed files in S3
            # TODO

            # Obtain file date
            if self.incoming_file_config.csv_specs.date_located == 'filename':
                pattern = self.incoming_file_config.csv_specs.date_located_filename.regex
                match = re.search(pattern, self.config.parameters.source_file_path)
                year, month, day = match.groups()
                file_date = f'{year}-{month}-{day}'
            elif self.incoming_file_config.csv_specs.date_located == 'column':
                # TODO
                file_date = ''

            # Create partitions
            if self.output_file_config.partitions.datadate:
                # Partition by date of the file
                response = self.catalogue.create_partition(
                    self.output_file_config.database,
                    self.output_file_config.table,
                    'datadate',
                    file_date
                )
            elif self.output_file_config.partitions.insert_time:
                # Partition by insertion timestamp
                insert_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                response = self.catalogue.create_partition(
                    self.output_file_config.database,
                    self.output_file_config.table,
                    'insert_time',
                    insert_time
                )

            # Save file in raw table
            # TODO

    def read_data(self) -> dict:
        s3_file_content = self.storage.read(self.config.parameters.source_file_path)
        file_contents = {}
        if self.incoming_file_config.zipped == 'zip':
            # TODO: other types of compressed files
            with ZipFile(s3_file_content, 'r') as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        file_contents[filename] = BytesIO(f.read())
        else:
            filename = Path(self.config.parameters.source_file_path).name
            file_contents[filename] = BytesIO(s3_file_content)
        return file_contents

    def validate_file(self, file_contents: dict) -> bool:
        try:
            if self.validations.validate_extension:
                self.validate_extension(file_contents)
            if self.validations.validate_filename:
                self.validate_filename(file_contents)
            if self.validations.validate_csv:
                self.validate_csv(file_contents)
        except AssertionError:
            self.logger.info(f'File {self.config.parameters.source_file_path} is invalid')
            return False
        except Exception as e:
            self.logger.error(f'Error validating file {self.config.parameters.source_file_path}: {e}')
            return False
        else:
            self.logger.info(f'File {self.config.parameters.source_file_path} is valid')
            return True

    def validate_extension(self, file_contents: dict) -> None:
        extension = Path(self.config.parameters.source_file_path).suffix.strip('.').lower()
        if self.incoming_file_config.zipped:
            # Compressed file
            expected_extension = self.incoming_file_config.zipped
            assert extension == expected_extension
            # Validate extension of the files inside the compressed file
            expected_extension = self.incoming_file_config.file_format
            for filename in file_contents.keys():
                # Assumes that all the files inside the compressed file have the same extension
                extension = Path(filename).suffix.strip('.').lower()
                assert extension == expected_extension
        else:
            # Uncompressed file
            expected_extension = self.incoming_file_config.file_format
            assert extension == expected_extension

    def validate_filename(self, file_contents: dict) -> None:
        pattern = self.incoming_file_config.filename_pattern
        filename = Path(self.config.parameters.source_file_path).name.split('.')[0]
        assert bool(re.match(pattern, filename))
        if self.incoming_file_config.zipped:
            for filename in file_contents.keys():
                # Assumes that all the files inside the compressed file have the same pattern
                assert bool(re.match(pattern, filename))

    def validate_csv(self, file_contents: dict) -> bool:
        for filename, content in file_contents.items():
            self.logger.info(f'Validating {filename} contents')
            df = pd.read_csv(
                content,
                delimiter=self.incoming_file_config.csv_specs.delimiter,
                header=self.incoming_file_config.csv_specs.header_position,
                encoding=self.incoming_file_config.csv_specs.encoding
            )
            # TODO: no contar líneas vacías al final del fichero
            expected_n_rows = content.getvalue().count(b'\n') - self.incoming_file_config.csv_specs.header_position
            expected_n_columns = self.get_expected_number_of_columns(content)
            assert df.shape == (expected_n_rows, expected_n_columns)
            if self.validations.validate_columns:
                self.validate_columns(df)

    def validate_columns(self, df: DataFrame) -> bool:
        columns = list(df.columns)
        expected_columns = self.catalogue.get_schema(
            self.output_file_config.database,
            self.output_file_config.table
        )
        if self.incoming_file_config.ordered_columns:
            assert columns == expected_columns
        else:
            extra_columns = list(set(columns) - set(expected_columns))
            missing_columns = list(set(expected_columns) - set(columns))
            diff_columns = extra_columns + missing_columns
            assert len(diff_columns) == 0

    def get_expected_number_of_columns(self, csv_content: BytesIO) -> int:
        csv_content.seek(0)
        for i, line in enumerate(csv_content):
            if i == self.incoming_file_config.csv_specs.header_position:
                encoding = self.incoming_file_config.csv_specs.encoding
                delimiter = self.incoming_file_config.csv_specs.delimiter
                return len(line.decode(encoding).split(delimiter))

    def _transformations(self, df):
        pass

    def _write_data(self, df):
        pass


if __name__ == '__main__':
    stb = ProcessingCoordinator()
    stb.process()
