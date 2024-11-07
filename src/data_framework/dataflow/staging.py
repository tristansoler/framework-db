from data_framework.modules.config.core import config, Config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.validation.quality_controls import QualityControls
from typing import Any, Dict


class RawToStaging:

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

    @property
    def source_tables(self) -> QualityControls:
        return self.__source_tables

    @property
    def target_tables(self) -> QualityControls:
        return self.__target_tables

    def __init__(self):
        self.__config = config()
        self.__logger = logger
        self.__data_process = CoreDataProcess()
        self.__quality_controls = QualityControls()
        self.__source_tables = self.__config.processes.raw_to_staging.source_tables
        self.__target_tables = self.__config.processes.raw_to_staging.target_tables

    def read_table_with_casting(self, input_table_key: str, casting_table_key: str) -> Any:
        input_table = self.source_tables.table(input_table_key)
        casting_table = self.target_tables.table(casting_table_key)
        partition = f'{input_table.partition_field}={self.config.parameters.file_date}'
        response = self.data_process.datacast(
            input_table.database_relation,
            input_table.table,
            casting_table.database_relation,
            casting_table.table,
            input_table.partition_field,
            self.config.parameters.file_date
        )
        if response.success:
            df = response.data
            self.logger.info(f'Read {df.count()} rows from {input_table.full_name} with partition {partition}')
            return df
        else:
            self.logger.error(
                f'Error reading data from {input_table.full_name} with partition {partition}: {response.error}'
            )
            raise response.error

    def read_all_source_tables(self, _filter: str = None) -> Dict[str, Any]:
        tables_content = {}
        for table_key, table_config in self.source_tables.tables.items():
            response = self.data_process.read_table(
                table_config.database_relation, table_config.table, _filter
            )
            tables_content[table_key] = response.data
            if not response.success:
                self.logger.error(
                    f'Error reading data from {table_config.full_name}: {response.error}'
                )
        return tables_content

    def write(self, df: Any, output_table_key: str) -> None:
        output_table = self.target_tables.table(output_table_key)
        response = self.data_process.merge(
            df,
            output_table.database_relation,
            output_table.table,
            # TODO: obtain primary keys from Glue table
            output_table.primary_keys
        )
        if response.success:
            self.logger.info(f'Successfully inserted data into {output_table.full_name}')
        else:
            self.logger.error(f'Error inserting data into {output_table.full_name}: {response.error}')
            raise response.error


if __name__ == '__main__':
    staging = RawToStaging()
