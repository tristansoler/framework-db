from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import OutputReport
from data_framework.modules.utils.logger import logger
from pyspark.sql import DataFrame
from datetime import datetime


class ProcessingCoordinator:

    def __init__(self):
        self.config = config()
        self.current_process_config = self.config.current_process_config()
        self.logger = logger
        self.data_process = CoreDataProcess()

    def process(self) -> dict:
        # Build generic response
        response = {
            'success': [],
            'fail': [],
            'errors': [],
        }
        try:
            # Generate all outputs
            for config_output in self.current_process_config.output_reports:
                try:
                    self.generate_output_file(config_output)
                except Exception as e:
                    response['fail'].append(config_output.name)
                    response['errors'].append(f'Error generating output {config_output.name}: {str(e)}')
                else:
                    response['success'].append(config_output.name)
            return response
        except Exception as e:
            self.logger.error(f'Error generating outputs: {e}')
            response['fail'].append(f'Error generating outputs: {e}')
            return response

    def generate_output_file(self, config_output: OutputReport) -> None:
        self.logger.info(f'Generating output {config_output.name}')
        # Obtain data
        df = self.retrieve_data(config_output)
        # Upload output data to S3
        self.write_data_to_file(df, config_output)
        self.logger.info(f'Output {config_output.name} generated successfully')

    def retrieve_data(self, config_output: OutputReport) -> DataFrame:
        """
        Function to build sql a retrieve the dataframe with the data
        """
        if config_output.columns_alias:
            columns = [
                f"{column} as {column_alias}"
                for column, column_alias in zip(config_output.columns, config_output.columns_alias)
            ]
        else:
            columns = config_output.columns
        _filter = self.format_string(config_output.where)
        self.logger.info(
            f'Obtaining data from {config_output.source_table.full_name} with filter {_filter}'
        )
        df = self.data_process.read_table(
            config_output.source_table.database, config_output.source_table.table, _filter, columns
        )
        return df

    def write_data_to_file(self, df: DataFrame, config_output: OutputReport) -> None:
        """
        Function to write the dataframe with the data in storage
        """
        # TODO: obtener nombre del bucket usando el FW
        bucket_output = f'{self.config.parameters.bucket_prefix}-{Layer.OUTPUT.value}'
        filename = self.format_string(config_output.filename_pattern, config_output.filename_date_format)
        filename_path = f"s3://{bucket_output}/funds_output/{self.config.parameters.dataflow}/{filename}"
        self.logger.info(f'Saving output {config_output.name} in {filename_path}')
        if config_output.file_format == "csv":
            header = config_output.csv_specs['header']
            delimiter = config_output.csv_specs['delimiter']
            df.write.options(header=header, delimiter=delimiter) \
                .csv(filename_path)
        # TODO: Salida a excel y json

    def format_string(self, string_to_format: str, date_format: str = '%Y-%m-%d') -> str:
        # TODO: permitir argumentos custom (p.ej. country en JPM)
        formatted_string = string_to_format.format(
            file_date=self.config.parameters.file_date,
            current_date=datetime.now().strftime(date_format)
        )
        return formatted_string


if __name__ == '__main__':
    output = ProcessingCoordinator()
    response = output.process()
