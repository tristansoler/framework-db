from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from typing import Any


class RawToStaging:

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.data_process = CoreDataProcess()
        self.incoming_file_config = self.config.processes.raw_to_staging.incoming_file
        self.output_file_config = self.config.processes.raw_to_staging.output_file
        self.df = self.read()

    def read(self) -> Any:
        partition = f'{self.incoming_file_config.partition_field}={self.config.parameters.file_date}'
        input_table = f'{self.incoming_file_config.database_relation}.{self.incoming_file_config.table}'
        response = self.data_process.datacast(
            self.incoming_file_config.database_relation,
            self.incoming_file_config.table,
            self.output_file_config.database_relation,
            self.output_file_config.table,
            self.incoming_file_config.partition_field,
            self.config.parameters.file_date
        )
        if response.success:
            df = response.data
            self.logger.info(f'Read {df.count()} rows from {input_table} with partition {partition}')
            return df
        else:
            self.logger.error(f'Error reading data from {input_table} with partition {partition}: {response.error}')

    def write(self) -> None:
        output_table = f'{self.output_file_config.database_relation}.{self.output_file_config.table}'
        response = self.data_process.merge(
            self.df,
            self.output_file_config.database_relation,
            self.output_file_config.table,
            # TODO: obtain primary keys from Glue table
            self.output_file_config.primary_keys
        )
        if response.success:
            self.logger.info(f'Successfully inserted data into {output_table}')
        else:
            self.logger.error(f'Error inserting data into {output_table}: {response.error}')


if __name__ == '__main__':
    staging = RawToStaging()
    staging.write()
