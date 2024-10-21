import re
from datetime import datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import tarfile
import pandas as pd
from pandas import DataFrame
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.storage.interface_storage import Layer, Database


class FileValidator:

    def __init__(self, file_contents: dict):
        self.config = config()
        self.logger = logger
        self.catalogue = CoreCatalogue._catalogue
        self.incoming_file_config = self.config.processes.landing_to_raw.incoming_file
        self.output_file_config = self.config.processes.landing_to_raw.output_file
        self.validations = self.config.processes.landing_to_raw.incoming_file.validations
        self.file_contents = file_contents

    def validate_file(self) -> None:
        # TODO: control metrics
        is_valid = True
        if self.validations.validate_extension:
            valid_extension = self.validate_extension()
            is_valid = is_valid and valid_extension
        if self.validations.validate_filename:
            valid_filename = self.validate_filename()
            is_valid = is_valid and valid_filename
        if self.validations.validate_csv:
            valid_csv = self.validate_csv()
            is_valid = is_valid and valid_csv
        return is_valid

    def validate_extension(self) -> bool:
        try:
            extension = Path(self.config.parameters.source_file_path).suffix.strip('.').lower()
            if self.incoming_file_config.zipped:
                # Compressed file
                expected_extension = self.incoming_file_config.zipped
                assert extension == expected_extension
                # Validate extension of the files inside the compressed file
                expected_extension = self.incoming_file_config.file_format
                for filename in self.file_contents.keys():
                    # Assumes that all the files inside the compressed file have the same extension
                    extension = Path(filename).suffix.strip('.').lower()
                    assert extension == expected_extension
            else:
                # Uncompressed file
                expected_extension = self.incoming_file_config.file_format
                assert extension == expected_extension
        except AssertionError:
            self.logger.info(f'Extension of the file {self.config.parameters.source_file_path} is invalid')
            return False
        except Exception as e:
            self.logger.error(f'Error validating extension of the file {self.config.parameters.source_file_path}: {e}')
            return False
        else:
            self.logger.info(f'Extension of the file {self.config.parameters.source_file_path} is valid')
            return True

    def validate_filename(self) -> bool:
        try:
            pattern = self.incoming_file_config.filename_pattern
            filename = Path(self.config.parameters.source_file_path).name.split('.')[0]
            assert bool(re.match(pattern, filename))
            if self.incoming_file_config.zipped:
                for filename in self.file_contents.keys():
                    # Assumes that all the files inside the compressed file follow the same pattern
                    assert bool(re.match(pattern, filename))
        except AssertionError:
            self.logger.info(f'Name of the file {self.config.parameters.source_file_path} is invalid')
            return False
        except Exception as e:
            self.logger.error(f'Error validating name of the file {self.config.parameters.source_file_path}: {e}')
            return False
        else:
            self.logger.info(f'Name of the file {self.config.parameters.source_file_path} is valid')
            return True

    def validate_csv(self) -> bool:
        for filename, content in self.file_contents.items():
            try:
                df = pd.read_csv(
                    content,
                    dtype=str,
                    delimiter=self.incoming_file_config.csv_specs.delimiter,
                    header=self.incoming_file_config.csv_specs.header_position,
                    encoding=self.incoming_file_config.csv_specs.encoding
                )
                expected_n_rows = self.get_expected_number_of_rows(content)
                expected_n_columns = self.get_expected_number_of_columns(content)
                assert df.shape == (expected_n_rows, expected_n_columns)
            except AssertionError:
                self.logger.info(f'Header and/or separator of the file {filename} are invalid')
                return False
            except Exception as e:
                self.logger.error(f'Error validating header and separator of the file {filename}: {e}')
                return False
            else:
                self.logger.info(f'Header and separator of the file {filename} are valid')
                if self.validations.validate_columns:
                    return self.validate_columns(filename, df)
                return True

    def validate_columns(self, filename: str, df: DataFrame) -> bool:
        try:
            columns = self.parse_columns(df)
            # TODO: ignore partition columns
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
        except AssertionError:
            self.logger.info(f'Columns of the file {filename} are invalid')
            return False
        except Exception as e:
            self.logger.error(f'Error validating columns of the file {filename}: {e}')
            return False
        else:
            self.logger.info(f'Columns of the file {filename} are valid')
            return True

    def get_expected_number_of_rows(self, csv_content: BytesIO) -> int:
        csv_content.seek(0)
        header_position = self.incoming_file_config.csv_specs.header_position
        lines = csv_content.getvalue().splitlines()
        return len(lines) - (header_position + 1)

    def get_expected_number_of_columns(self, csv_content: BytesIO) -> int:
        csv_content.seek(0)
        header_position = self.incoming_file_config.csv_specs.header_position
        for i, line in enumerate(csv_content):
            if i == header_position:
                encoding = self.incoming_file_config.csv_specs.encoding
                delimiter = self.incoming_file_config.csv_specs.delimiter
                return len(line.decode(encoding).split(delimiter))

    def parse_columns(self, df: DataFrame) -> list:
        # TODO: parametrizar en config
        # Replace whitespaces with _ and remove special characters
        columns = [
            re.sub(
                r'\s+', '_',
                re.sub(
                    r'[^A-Za-z0-9\s_]', '',
                    column.lower().strip().replace('/', ' ')
                )
            )
            for column in df.columns
        ]
        return columns


class ProcessingCoordinator:

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.storage = Storage._storage
        self.catalogue = CoreCatalogue._catalogue
        self.incoming_file_config = self.config.processes.landing_to_raw.incoming_file
        self.output_file_config = self.config.processes.landing_to_raw.output_file

    def process(self) -> dict:
        try:
            # Build generic response
            response = {
                'success': None,
                'file-name': Path(self.config.parameters.source_file_path).name,
                'file-date': None
            }
            # Read file from S3
            file_contents = self.read_data()
            # Apply controls
            file_validator = FileValidator(file_contents)
            is_valid = file_validator.validate_file()
            if is_valid:
                # Obtain file date
                file_date = self.obtain_file_date()
                # Create partitions
                partitions = self.create_partitions(file_date)
                # Save file in raw table
                self.write_data(file_contents, partitions)
                # Send response
                response['success'] = True
                response['file-date'] = file_date
                return response
            else:
                response['success'] = False
                return response
        except Exception as e:
            self.logger.error(f'Error processing file {self.config.parameters.source_file_path}: {e}')
            response['success'] = False
            return response

    def read_data(self) -> dict:
        s3_file_content = BytesIO(
            self.storage.read_from_path(
                layer=Layer.LANDING,
                key_path=self.config.parameters.source_file_path
            ).data
        )
        file_contents = {}
        if self.incoming_file_config.zipped == 'zip':
            with ZipFile(s3_file_content, 'r') as z:
                for filename in z.namelist():
                    with z.open(filename) as f:
                        file_contents[filename] = BytesIO(f.read())
        elif self.incoming_file_config.zipped == 'tar':
            with tarfile.open(fileobj=s3_file_content, mode='r') as t:
                for filename in t.getnames():
                    content = t.extractfile(filename).read()
                    file_contents[filename] = BytesIO(content)
        else:
            filename = Path(self.config.parameters.source_file_path).name
            file_contents[filename] = s3_file_content
        return file_contents

    def obtain_file_date(self) -> str:
        if self.incoming_file_config.csv_specs.date_located == 'filename':
            pattern = self.incoming_file_config.csv_specs.date_located_filename.regex
            match = re.search(pattern, self.config.parameters.source_file_path)
            year, month, day = match.groups()
            return f'{year}-{month}-{day}'
        elif self.incoming_file_config.csv_specs.date_located == 'column':
            # TODO
            return ''

    def create_partitions(self, file_date: str) -> dict:
        partitions = {}
        if self.output_file_config.partitions.datadate:
            # Partition by date of the file
            # TODO: uncomment after create_partition implementation
            response = self.catalogue.create_partition(
                self.output_file_config.database,
                self.output_file_config.table,
                'datadate',
                file_date
            )
            # TODO: validate response
            partitions['datadate'] = file_date
        if self.output_file_config.partitions.insert_time:
            # Partition by insertion timestamp
            insert_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # TODO: uncomment after create_partition implementation
            response = self.catalogue.create_partition(
                self.output_file_config.database,
                self.output_file_config.table,
                'insert_time',
                insert_time
            )
            # TODO: validate response
            partitions['insert_time'] = insert_time
        return partitions

    def write_data(self, file_contents: dict, partitions: dict) -> None:
        for filename, content in file_contents.items():
            self.storage.write(
                layer=Layer.RAW,
                # TODO: parametrizar database
                database=Database.FUNDS,
                table=self.output_file_config.table,
                data=content,
                partitions=partitions,
                filename=filename
            )


if __name__ == '__main__':
    stb = ProcessingCoordinator()
    stb.process()
