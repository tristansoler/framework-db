from data_framework.modules.dataflow.interface_dataflow import DataFlowInterface, OutputResponse
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.model.flows import OutputReport
from data_framework.modules.exception.output_exceptions import (
    OutputError,
    OutputGenerationError,
    NoOutputDataError
)
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
        # Generate all outputs
        for config_output in self.output_reports:
            try:
                self.generate_output_file(config_output)
            except Exception as e:
                error = OutputGenerationError(output_name=config_output.name, error_message=str(e))
                self.logger.error(error.format_exception())
                output = OutputResponse(
                    name=config_output.name,
                    success=False,
                    error=error
                )
                self.payload_response.outputs.append(output)
                self.payload_response.success = False
            else:
                output = OutputResponse(
                    name=config_output.name,
                    success=True
                )
                self.payload_response.outputs.append(output)
        if not self.payload_response.success:
            failed_outputs = self.payload_response.get_failed_outputs()
            raise OutputError(failed_outputs=failed_outputs)

    def generate_output_file(self, config_output: OutputReport) -> None:
        self.logger.info(f'Generating output {config_output.name}')
        # Obtain data
        df = self.retrieve_data(config_output)
        # Upload output data to S3
        if df and not df.isEmpty():
            self.write_data_to_file(df, config_output)
            self.logger.info(f'Output {config_output.name} generated successfully')
        else:
            raise NoOutputDataError(output_name=config_output.name)

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
        if config_output.where:
            _filter = self.format_string(config_output.where)
            self.logger.info(
                f'Obtaining data from {config_output.source_table.full_name} with filter {_filter}'
            )
        else:
            _filter = None
            self.logger.info(
                f'Obtaining data from {config_output.source_table.full_name}'
            )
        response = self.data_process.read_table(
            config_output.source_table.database_relation, config_output.source_table.table, _filter, columns
        )
        return response.data

    def write_data_to_file(self, df: DataFrame, config_output: OutputReport) -> None:
        """
        Function to write the dataframe with the data in storage
        """
        filename = self.format_string(config_output.filename_pattern, config_output.filename_date_format)
        output_folder = self.parse_output_folder(config_output.name)
        file_path = f"{self.config.project_id}/{output_folder}/inbound/{filename}"
        self.logger.info(f"Saving output '{config_output.name}' in {file_path}")

        file_to_save = BytesIO()

        if config_output.csv_specs:
            pdf = df.toPandas()
            pdf.to_csv(
                file_to_save,
                sep=config_output.csv_specs.delimiter,
                header=config_output.csv_specs.header,
                index=config_output.csv_specs.index,
                encoding=config_output.csv_specs.encoding
            )

        if config_output.json_specs:
            response = Storage.base_layer_path(layer=Layer.OUTPUT)
            tmp_write_path = f"{response.base_path}/{self.config.project_id}/{output_folder}/tmp/"
            df.coalesce(1).write.mode("overwrite").json(path=tmp_write_path)

            tmp_read_path = f"{self.config.project_id}/{output_folder}/tmp/"
            response = Storage.list_files(layer=Layer.OUTPUT, prefix=tmp_read_path)
            path_output_file = next((path for path in response.result if ".json" in path), "")
            response = Storage.read(layer=Layer.OUTPUT, key_path=path_output_file)

            file = response.data

            if config_output.replaces:
                file = file.decode('utf-8')
                for replace_dict in config_output.replaces:
                    for replace, new_value in replace_dict.items():
                        file = file.replace(replace, new_value)
                file = file.encode('utf-8')

            file_to_save = BytesIO(file)

        response = self.storage.write_to_path(Layer.OUTPUT, file_path, file_to_save.getvalue())
        if response.success:
            self.notifications.send_notification(
                notification_name='output_generated',
                arguments={
                    'dataflow': self.config.parameters.dataflow,
                    'process': self.config.parameters.process,
                    'output_name': config_output.name,
                    'file_path': file_path
                }
            )

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
