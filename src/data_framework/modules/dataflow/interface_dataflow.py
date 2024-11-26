from abc import ABC, abstractmethod
from data_framework.modules.config.core import config, Config
from data_framework.modules.config.model.flows import TableDict, DatabaseTable
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.validation.quality_controls import QualityControls
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List
import boto3
import json

@dataclass
class DataQualityTable:
    database: str
    table: str

@dataclass
class DataQuality:
    tables: List[DataQualityTable] = field(default_factory=list)

@dataclass
class OutputResult:
    name: str
    success: bool = False
    error: str = None

@dataclass
class PayloadResponse:
    success: bool = False
    next_stage: bool = False
    file_name: str = None
    file_date: str = None
    data_quality: DataQuality = field(default_factory=DataQuality)
    outputs: List[OutputResult] = field(default_factory=list)

class DataFlowInterface(ABC):
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
    def source_tables(self) -> TableDict:
        return self.__current_process_config.source_tables

    @property
    def target_tables(self) -> TableDict:
        return self.__current_process_config.target_tables

    @property
    def payload_response(self) -> PayloadResponse:
        return self.__payload_response

    @property
    def incoming_file(self) -> DatabaseTable:
        return self.__current_process_config.incoming_file

    @property
    def output_file(self) -> DatabaseTable:
        return self.__current_process_config.output_file

    def __init__(self):
        self.__config = config()
        self.__current_process_config = self.__config.current_process_config()
        self.__logger = logger
        self.__data_process = CoreDataProcess()
        self.__quality_controls = QualityControls()
        self.__payload_response = PayloadResponse()
        self.__ssm_client = boto3.client('ssm', region_name=self.config.parameters.region)

    def process(self):
        message = "It is mandatory to implement this function"
        self.logger.error(message)
        raise message

    def read_table_with_casting(
        self,
        name_of_raw_table: str,
        name_of_staging_table_to_casting: str = None
    ) -> Any:
        input_table = self.source_tables.table(name_of_raw_table)
        name_of_staging_table_to_casting = (
            name_of_staging_table_to_casting
            if name_of_staging_table_to_casting
            else name_of_raw_table
        )
        casting_table = self.target_tables.table(name_of_staging_table_to_casting)

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

    def read_table(self, name_of_table: str) -> Any:
        input_table = self.source_tables.table(name_of_table)

        partition = f'{input_table.partition_field}={self.config.parameters.file_date}'

        response = self.data_process.read_table(
            database=input_table.database_relation,
            table=input_table.table,
            filter=input_table.sql_where
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

    def save_payload_response(self):
        if self.config.parameters.process == 'landing_to_raw':
            dq_table = DataQualityTable(
                database=self.__current_process_config.output_file.database,
                table=self.__current_process_config.output_file.table
            )
            self.payload_response.data_quality.tables.append(dq_table)
        elif self.config.parameters.process != 'business_to_output':
            for tale_name in self.__current_process_config.target_tables.tables:
                table_info = self.__current_process_config.target_tables.table(table_key=tale_name)

                dq_table = DataQualityTable(
                    database=table_info.database,
                    table=table_info.table
                )

                self.payload_response.data_quality.tables.append(dq_table)

        payload_json = json.dumps(asdict(self.payload_response), ensure_ascii=False, indent=2)

        ssm_name = f'/dataflow/{self.config.project_id}/{self.config.parameters.dataflow}-{self.config.parameters.process}/result'

        self.__ssm_client.put_parameter(
            Name=ssm_name,
            Value=payload_json,
            Type='String',
            Overwrite=True
        )

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