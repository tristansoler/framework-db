from data_framework.modules.config.core import config, Config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.validation.quality_controls import QualityControls
from typing import Any


class RawToStaging:

    @property
    def origin_dataframe(self):
        return self.__df

    @property
    def config(self) -> Config:
        return self.__config

    @property
    def logger(self):
        return self.__logger

    @property
    def data_process(self) -> CoreDataProcess:
        return self.__data_process

    @property
    def quality_controls(self) -> QualityControls:
        return self.__quality_controls

    def __init__(self):
        self.__config = config()
        self.__logger = logger
        self.__data_process = CoreDataProcess()
        self.__incoming_file_config = self.__config.processes.raw_to_staging.incoming_file
        self.__output_file_config = self.__config.processes.raw_to_staging.output_file
        self.__df = self.read()
        self.__quality_controls = QualityControls()

    def read(self) -> Any:
        partition = f'{self.__incoming_file_config.partition_field}={self.__config.parameters.file_date}'
        input_table = f'{self.__incoming_file_config.database_relation}.{self.__incoming_file_config.table}'

        response = self.__data_process.read_table_with_filter(
            database=self.__incoming_file_config.database_relation,
            table=self.__incoming_file_config.table,
            _filter=partition
        )
        
        if response.success:
            df = response.data
            self.__logger.info(f'Read {df.count()} rows from {input_table} with partition {partition}')
            
            return df
        else:
            self.__logger.error(f'Error reading data from {input_table} with partition {partition}: {response.error}')

    def write(self) -> None:
        output_table = f'{self.__output_file_config.database_relation}.{self.__output_file_config.table}'
        response = self.__data_process.merge(
            self.__df,
            self.__output_file_config.database_relation,
            self.__output_file_config.table,
            # TODO: obtain primary keys from Glue table
            self.__output_file_config.primary_keys
        )
        if response.success:
            self.__logger.info(f'Successfully inserted data into {output_table}')
        else:
            self.__logger.error(f'Error inserting data into {output_table}: {response.error}')

    def apply_quality_controls(self) -> None:
        df = self.__df
        database = self.__config.processes.raw_to_staging.incoming_file.database_relation
        table = self.__config.processes.raw_to_staging.incoming_file.table
        self.__quality_controls.validate(df, database, table)


if __name__ == '__main__':
    staging = RawToStaging()
