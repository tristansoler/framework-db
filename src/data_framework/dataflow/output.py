from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from pyspark.sql import DataFrame
from datetime import datetime

# TODO: FALTA POR PROBAR el flujo completo ya que no funciona spark
# TODO: Salida a excel y json


class MakeOutput():

    def __init__(self):
        self.logger = logger
        self.data_process = CoreDataProcess()

    def set_config_output(self, config_parameters, config_output):
        """
        Function to set parameters
        """
        self.name = config_output['name']
        self.database = config_output['database_relation']
        self.table = config_output['table']
        self.columns = config_output['columns']
        self.columns_alias = config_output['columns_alias']
        self.where = config_output['where']
        self.file_format = config_output['file_format']
        self.filename_pattern = config_output['filename_pattern']
        self.csv_specs = config_output['csv_specs']
        self.bucket_prefix = config_parameters.bucket_prefix
        self.process = config_parameters.process

    def retrieve_data(self) -> DataFrame:
        """
        Function to build sql a retrieve the dataframe with the data
        """
        columns = dict(zip(self.columns, self.columns_alias))
        l_columns = [f"{key} as {val}" for key, val in columns.items()]
        df = self.data_process.read_table(self.database, self.table, self.where, l_columns)
        return df

    def write_data_to_file(self, df: DataFrame):
        """
        Function to write the dataframe with the data in storage
        """
        try:
            today = datetime.now()
            bucket_output = self.bucket_prefix + "-output"
            file_output_path = f"funds_output/{self.process}"
            filename = self.filename_pattern.format(today.strftime('%Y%m%d'))
            filename = f"s3://{bucket_output}/{file_output_path}/{filename}"
            if self.file_format == "csv":
                header = self.csv_specs['header']
                delimiter = self.csv_specs['delimiter']
                df.write.options(header=header, delimiter=delimiter) \
                    .csv(filename)
            success = True
        except Exception as e:
            self.logger.error(f'Error write_data_to_file : {e}')
            success = False
        return success


class ProcessingCoordinator:

    def __init__(self):
        self.config = config()
        self.current_process_config = self.config.current_process_config()
        self.logger = logger
        self.make_output = MakeOutput()

    def process(self) -> dict:

        # Build generic response
        response = {
            'success': [],
            'fail': [],
            'errors': [],
        }

        try:
            l_success = []
            l_fail = []
            l_fail_errors = []

            # Loop for all output
            l_outputs = self.current_process_config.output_reports
            for config_output in l_outputs:
                report_name = config_output['name']
                # Set config values
                self.make_output.set_config_output(self.config.parameters, config_output)
                try:
                    # Retrieve data to output
                    df = self.make_output.retrieve_data()
                    # Send file to bucket_output/folder
                    success = self.make_output.write_data_to_file(df)
                    if success:
                        l_success.append(report_name)
                except Exception as e:
                    l_fail.append(report_name)
                    l_fail_errors.append(f'El output {report_name} ha dado error: {str(e)}')

            response['success'] = l_success
            response['fail'] = l_fail
            response['errors'] = l_fail_errors

            return response

        except Exception as e:
            self.logger.error(f'Error output: {e}')
            response['fail'].append(f'Error output: {e}')
            return response


if __name__ == '__main__':
    stb = ProcessingCoordinator()
    response = stb.process()
