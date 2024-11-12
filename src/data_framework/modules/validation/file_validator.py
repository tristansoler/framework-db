from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
import re
from io import BytesIO
from pathlib import Path
import pandas as pd
from pandas import DataFrame


class FileValidator:

    def __init__(self, file_contents: dict):
        self.config = config()
        self.current_process_config = self.config.current_process_config()
        self.logger = logger
        self.catalogue = CoreCatalogue()
        self.incoming_file_config = self.current_process_config.incoming_file
        self.output_file_config = self.current_process_config.output_file
        self.validations = self.current_process_config.incoming_file.validations
        self.file_contents = file_contents
        self.file_name = Path(self.config.parameters.source_file_path).name

    def validate_file(self) -> None:
        is_valid = True

        if self.validations.validate_filename:
            valid_filename = self.validate_filename()
            is_valid = is_valid and valid_filename
        if self.validations.validate_csv:
            valid_csv = self.validate_csv()
            is_valid = is_valid and valid_csv
        return is_valid

    def validate_filename(self) -> bool:
        return_value = False

        try:
            pattern = self.incoming_file_config.filename_pattern
            assert bool(
                re.match(pattern, self.file_name)
            ), f"Name of the file '{self.file_name}' is invalid"

            if self.incoming_file_config.zipped:
                for filename, file_data in self.file_contents.items():
                    if file_data['validate']:
                        assert bool(
                            re.match(self.incoming_file_config.filename_unzipped_pattern, filename)
                        ), f"The name of the unzipped file '{filename}' is incorrect"
        except AssertionError as error:
            self.logger.error(error)
        except Exception as error:
            self.logger.error(f"Error validating name of the file '{self.file_name}': {error}")
        else:
            self.logger.info(f"Name of the file '{self.file_name}' is valid")
            return_value = True

        return return_value

    def validate_csv(self) -> bool:
        for filename, file_data in self.file_contents.items():
            if file_data['validate']:
                try:
                    df = pd.read_csv(
                        file_data['content'],
                        dtype=str,
                        delimiter=self.incoming_file_config.csv_specs.delimiter,
                        header=self.incoming_file_config.csv_specs.header_position,
                        encoding=self.incoming_file_config.csv_specs.encoding
                    )
                    expected_n_rows = self.get_expected_number_of_rows(file_data['content'])
                    expected_n_columns = self.get_expected_number_of_columns(file_data['content'])
                    assert df.shape == (expected_n_rows, expected_n_columns)
                except AssertionError:
                    self.logger.error(f'Header and/or separator of the file {filename} are invalid')
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
            # Obtain expected columns from raw table
            response = self.catalogue.get_schema(
                self.output_file_config.database_relation,
                self.output_file_config.table
            )
            expected_columns = response.schema.get_column_names(partitioned=False)
            extra_columns = list(set(columns) - set(expected_columns))
            missing_columns = list(set(expected_columns) - set(columns))
            diff_columns = extra_columns + missing_columns
            assert len(diff_columns) == 0
        except AssertionError:
            self.logger.error(f'Columns of the file {filename} are invalid')
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
        # TODO: definir un estándar y parametrizar en config
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
