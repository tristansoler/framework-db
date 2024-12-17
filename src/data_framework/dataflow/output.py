from data_framework.modules.dataflow.interface_dataflow import DataFlowInterface, OutputResponse
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.model.flows import OutputReport
from pyspark.sql import DataFrame
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
import re

TIME_ZONE = ZoneInfo('Europe/Madrid')


class ProcessingCoordinator(DataFlowInterface):

    def __init__(self):
        super().__init__()
        self.storage = Storage()

    def process(self) -> dict:

        self.payload_response.success = True

        try:
            # Generate all outputs
            for config_output in self.output_reports:
                try:
                    self.generate_output_file(config_output)
                except Exception as e:
                    self.logger.error(f'Error generating output {config_output.name}: {e}')
                    output = OutputResponse(
                        name=config_output.name,
                        success=False,
                        error=e
                    )
                    self.payload_response.outputs.append(output)
                    self.payload_response.success = False
                else:
                    output = OutputResponse(
                        name=config_output.name,
                        success=True
                    )
                    self.payload_response.outputs.append(output)
        except Exception as e:
            self.logger.error(f'Error generating outputs: {e}')
            self.payload_response.success = False
            raise e
        else:
            if not self.payload_response.success:
                failed_outputs = self.payload_response.get_failed_outputs()
                error_message = (
                    f"Error generating the following outputs: {', '.join(failed_outputs)}. " +
                    "Check logs for more information"
                )
                self.logger.error(error_message)
                raise Exception(error_message)

    def generate_output_file(self, config_output: OutputReport) -> None:
        self.logger.info(f'Generating output {config_output.name}')
        # Obtain data
        df = self.retrieve_data(config_output)
        # Upload output data to S3
        if df and not df.isEmpty():
            self.write_data_to_file(df, config_output)
            self.logger.info(f'Output {config_output.name} generated successfully')
        else:
            raise ValueError(f'No data available for output {config_output.name}')

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
        response = self.data_process.read_table(
            config_output.source_table.database_relation, config_output.source_table.table, _filter, columns
        )
        if response.success:
            return response.data
        else:
            self.logger.error(f'Error reading data: {response.error}')
            raise response.error

    def write_data_to_file(self, df: DataFrame, config_output: OutputReport) -> None:
        """
        Function to write the dataframe with the data in storage
        """
        filename = self.format_string(config_output.filename_pattern, config_output.filename_date_format)
        output_folder = self.parse_output_folder(config_output.name)
        file_path = f"{self.config.project_id}/{output_folder}/inbound/{filename}"
        self.logger.info(f'Saving output {config_output.name} in {file_path}')
        if config_output.file_format == "csv":
            csv_file = BytesIO()
            pdf = df.toPandas()
            pdf.to_csv(
                csv_file,
                sep=config_output.csv_specs.delimiter,
                header=config_output.csv_specs.header,
                index=config_output.csv_specs.index,
                encoding=config_output.csv_specs.encoding,
            )
            response = self.storage.write_to_path(Layer.OUTPUT, file_path, csv_file.getvalue())
            if not response.success:
                raise response.error
        # TODO: Salida a excel y json

    @staticmethod
    def parse_output_folder(output_folder: str) -> str:
        return re.sub(
            r'\s+', '_',
            re.sub(
                r'[^a-z\s]', '',
                output_folder.lower().strip()
            )
        )

    def format_string(self, string_to_format: str, date_format: str = '%Y-%m-%d') -> str:
        # TODO: permitir argumentos custom
        formatted_string = string_to_format.format(
            file_date=self.config.parameters.file_date,
            file_name=self.config.parameters.file_name,
            current_date=datetime.now(TIME_ZONE).strftime(date_format)
        )
        return formatted_string


if __name__ == '__main__':
    output = ProcessingCoordinator()
    response = output.process()
