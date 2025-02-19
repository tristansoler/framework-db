from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.validation.core_quality_controls import CoreQualityControls
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.validation.interface_quality_controls import ControlRule
import re
from io import BytesIO
from pathlib import Path
import pandas as pd


class FileValidator:

    def __init__(self, file_date: str, file_contents: dict, source_file_path: str):
        self.config = config()
        self.current_process_config = self.config.current_process_config()
        self.logger = logger
        self.catalogue = CoreCatalogue()
        self.quality_controls = CoreQualityControls()
        self.data_process = CoreDataProcess()
        self.incoming_file_config = self.current_process_config.incoming_file
        self.output_file_config = self.current_process_config.output_file
        self.file_contents = file_contents
        self.file_name = Path(source_file_path).name
        self.file_date = file_date

    def validate_filename_pattern(self, rule: ControlRule) -> None:
        pattern = self.incoming_file_config.filename_pattern
        valid_filename = bool(re.match(pattern, self.file_name))
        df_result = self.data_process.create_dataframe(
            pd.DataFrame({'identifier': [self.file_name], 'result': [valid_filename]})
        ).data
        result = rule.calculate_result(df_result)
        if result.valid_identifiers:
            rule.result.add_detail(f'Valid filenames: {self.file_name}')
        else:
            rule.result.add_detail(f'Invalid filenames: {self.file_name}. Expected pattern: {pattern}')
        rule.result.set_data_date(self.file_date)

    def validate_unzipped_filename_pattern(self, rule: ControlRule) -> None:
        pattern = self.incoming_file_config.filename_unzipped_pattern
        results = []
        filenames = []
        for filename, file_data in self.file_contents.items():
            if file_data['validate']:
                valid_filename = bool(re.match(pattern, filename))
                results.append(valid_filename)
                filenames.append(filename)

        df_result = self.data_process.create_dataframe(
            pd.DataFrame({'identifier': filenames, 'result': results})
        ).data
        result = rule.calculate_result(df_result)
        if result.valid_identifiers:
            rule.result.add_detail(f"Valid filenames: {', '.join(result.valid_identifiers)}")
        if result.invalid_identifiers:
            rule.result.add_detail(f"Invalid filenames: {', '.join(result.invalid_identifiers)}")
            rule.result.add_detail(f'Expected pattern: {pattern}')
        rule.result.set_data_date(self.file_date)

    def validate_csv_format(self, rule: ControlRule) -> None:
        delimiter = self.incoming_file_config.csv_specs.delimiter
        header = self.incoming_file_config.csv_specs.header_position
        encoding = self.incoming_file_config.csv_specs.encoding
        results = []
        filenames = []
        for filename, file_data in self.file_contents.items():
            if file_data['validate']:
                file_data['content'].seek(0)
                try:
                    df = pd.read_csv(
                        file_data['content'],
                        dtype=str,
                        delimiter=delimiter,
                        header=header,
                        encoding=encoding
                    )
                except Exception:
                    valid_csv = False
                else:
                    expected_n_columns = self._get_expected_number_of_columns(file_data['content'])
                    valid_csv = df.shape[1] == expected_n_columns
                results.append(valid_csv)
                filenames.append(filename)
        df_result = self.data_process.create_dataframe(
            pd.DataFrame({'identifier': filenames, 'result': results})
        ).data
        result = rule.calculate_result(df_result)
        if result.valid_identifiers:
            rule.result.add_detail(f"CSV files with valid format: {', '.join(result.valid_identifiers)}")
        if result.invalid_identifiers:
            rule.result.add_detail(f"CSV files with invalid format: {', '.join(result.invalid_identifiers)}")
            rule.result.add_detail(f'Expected separator: {delimiter}')
            rule.result.add_detail(f'Expected header position: {header}')
            rule.result.add_detail(f'Expected encoding: {encoding}')
        rule.result.set_data_date(self.file_date)

    def validate_csv_columns(self, rule: ControlRule) -> None:
        # Obtain expected columns from raw table
        response = self.catalogue.get_schema(
            self.output_file_config.database_relation,
            self.output_file_config.table
        )
        expected_columns = response.schema.get_column_names(partitioned=False)
        results = []
        filenames = []
        invalid_columns_info = []
        for filename, file_data in self.file_contents.items():
            if file_data['validate']:
                file_data['content'].seek(0)
                try:
                    df = pd.read_csv(
                        file_data['content'],
                        dtype=str,
                        delimiter=self.incoming_file_config.csv_specs.delimiter,
                        header=self.incoming_file_config.csv_specs.header_position,
                        encoding=self.incoming_file_config.csv_specs.encoding
                    )
                except Exception:
                    columns = []
                    valid_columns = False
                else:
                    columns = self._parse_columns(df)
                    valid_columns = columns == expected_columns
                results.append(valid_columns)
                filenames.append(filename)
                if not valid_columns:
                    extra_columns = list(set(columns) - set(expected_columns))
                    missing_columns = list(set(expected_columns) - set(columns))
                    invalid_columns_info.append(
                        f"{filename} (Extra columns: {', '.join(extra_columns) or None}, " +
                        f"Missing columns: {', '.join(missing_columns) or None})"
                    )
        df_result = self.data_process.create_dataframe(
            pd.DataFrame({'identifier': filenames, 'result': results})
        ).data
        result = rule.calculate_result(df_result)
        if result.valid_identifiers:
            rule.result.add_detail(f"CSV files with valid columns: {', '.join(result.valid_identifiers)}")
        if result.invalid_identifiers:
            rule.result.add_detail(f"CSV files with invalid columns: {', '.join(invalid_columns_info)}")
        rule.result.set_data_date(self.file_date)

    def _get_expected_number_of_columns(self, csv_content: BytesIO) -> int:
        csv_content.seek(0)
        header_position = self.incoming_file_config.csv_specs.header_position
        for i, line in enumerate(csv_content):
            if i == header_position:
                encoding = self.incoming_file_config.csv_specs.encoding
                delimiter = self.incoming_file_config.csv_specs.delimiter
                return len(line.decode(encoding).split(delimiter))

    def _parse_columns(self, df: pd.DataFrame) -> list:
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
