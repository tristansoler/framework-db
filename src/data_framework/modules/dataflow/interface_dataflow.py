from abc import ABC
from data_framework.modules.config.core import config, Config
from data_framework.modules.config.model.flows import (
    TableDict,
    DatabaseTable,
    OutputReport,
    ExecutionMode
)
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.validation.core_quality_controls import CoreQualityControls
from data_framework.modules.notification.core_notifications import CoreNotifications
from data_framework.modules.notification.interface_notifications import NotificationToSend
from data_framework.modules.monitoring.core_monitoring import (
    CoreMonitoring,
    Metric,
    MetricNames
)
from data_framework.modules.exception.dataflow_exceptions import (
    DataflowInitializationError,
    PayloadResponseError
)
from data_framework.modules.exception.aws_exceptions import SSMError
from dataclasses import dataclass, asdict, field
from typing import Any, List, Optional
import boto3
import json
import time


@dataclass
class DataQualityTable:
    database: str
    table: str


@dataclass
class DataQuality:
    tables: List[DataQualityTable] = field(default_factory=list)


@dataclass
class OutputResponse:
    name: str
    success: bool = False
    error: Any = None


@dataclass
class PayloadResponse:
    success: bool = False
    next_stage: bool = False
    file_name: str = None
    file_date: str = None
    data_quality: DataQuality = field(default_factory=DataQuality)
    outputs: List[OutputResponse] = field(default_factory=list)
    notifications: Optional[List[NotificationToSend]] = field(default_factory=list)

    def get_failed_outputs(self) -> List[str]:
        failed_outputs = [
            output.name
            for output in self.outputs
            if not output.success
        ]
        return failed_outputs


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
    def quality_controls(self) -> CoreQualityControls:
        return self.__quality_controls

    @property
    def notifications(self) -> CoreNotifications:
        return self.__notifications

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

    @property
    def output_reports(self) -> List[OutputReport]:
        return self.__current_process_config.output_reports

    def __init__(self):
        try:
            self.__config = config()
            self.__current_process_config = self.__config.current_process_config()
            self.__logger = logger
            self.__data_process = CoreDataProcess()
            self.__quality_controls = CoreQualityControls()
            self.__notifications = CoreNotifications()
            self.__payload_response = PayloadResponse()
            self.__ssm_client = boto3.client('ssm', region_name=self.config.parameters.region)
            self.__monitoring = CoreMonitoring()

            if self.config.is_first_process:
                self.__monitoring.track_process_metric(
                    name=MetricNames.DATAFLOW_START_EVENT,
                    value=1
                )

            self.__start_process = time.time()
        except Exception:
            raise DataflowInitializationError()

    def process(self):
        raise NotImplementedError('It is mandatory to implement process() function')

    def vars(self, name: str):
        return self.__current_process_config.vars.get_variable(name=name)

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
        execution_mode = self.config.parameters.execution_mode
        casting_table = self.target_tables.table(name_of_staging_table_to_casting)
        response = self.data_process.datacast(
            table_source=input_table,
            table_target=casting_table
        )
        df = response.data
        if execution_mode == ExecutionMode.FULL:
            self.logger.info(
                f'[ExecutionMode:{execution_mode.value}] Read from {input_table.full_name}'
            )
        else:
            self.logger.info(
                f"[ExecutionMode:{execution_mode.value}] Read {df.count()} rows " +
                f"from {input_table.full_name} with partition {input_table.sql_where}"
            )
        return df

    def read_table(self, name_of_table: str) -> Any:
        input_table = self.source_tables.table(name_of_table)
        execution_mode = self.config.parameters.execution_mode
        sql_where = input_table.sql_where
        if execution_mode == ExecutionMode.FULL.value:
            sql_where = None
        response = self.data_process.read_table(
            database=input_table.database_relation,
            table=input_table.table,
            filter=sql_where
        )
        df = response.data
        if execution_mode == ExecutionMode.FULL.value:
            self.logger.info(
                f'[ExecutionMode:{execution_mode}] Read {df.count()} rows from {input_table.full_name}'
            )
        else:
            self.logger.info(
                f"[ExecutionMode:{execution_mode}] Read {df.count()} rows " +
                f"from {input_table.full_name} with partition {sql_where}"
            )
        return df

    def write(self, df: Any, output_table_key: str) -> None:
        output_table = self.target_tables.table(output_table_key)
        self.data_process.merge(
            df,
            output_table.database_relation,
            output_table.table,
            # TODO: obtain primary keys from Glue table
            output_table.primary_keys
        )
        self.logger.info(f'Successfully inserted data into {output_table.full_name}')

    def save_monitorization(self):

        seconds = time.time() - self.__start_process

        self.__monitoring.track_process_metric(
            name=MetricNames.PROCESS_END_EVENT,
            value=1,
            success=True
        )

        self.__monitoring.track_process_metric(
            name=MetricNames.PROCESS_DURATION,
            value=seconds
        )

        # if self.config.has_next_process == False:
        #     self.__monitoring.track_process_metric(
        #         name=MetricNames.DATAFLOW_END_EVENT,
        #         value=1,
        #         success=True
        #     )

    def save_payload_response(self):
        try:
            if self.config.parameters.process == 'landing_to_raw':
                dq_table = DataQualityTable(
                    database=self.__current_process_config.output_file.database.value,
                    table=self.__current_process_config.output_file.table
                )
                self.payload_response.data_quality.tables.append(dq_table)
            elif self.config.parameters.process != 'business_to_output':
                for tale_name in self.__current_process_config.target_tables.tables:
                    table_info = self.__current_process_config.target_tables.table(table_key=tale_name)
                    dq_table = DataQualityTable(
                        database=table_info.database.value,
                        table=table_info.table
                    )

                    self.payload_response.data_quality.tables.append(dq_table)
            # Add notifications to send
            self.payload_response.notifications = self.__notifications.get_notifications_to_send()
            # Convert to JSON
            payload_json = json.dumps(asdict(self.payload_response), ensure_ascii=False, indent=2)
            try:
                ssm_name = (
                    f'/dataflow/{self.config.project_id}/' +
                    f'{self.config.parameters.dataflow}-{self.config.parameters.process}/result'
                )
                self.__ssm_client.put_parameter(
                    Name=ssm_name,
                    Value=payload_json,
                    Type='String',
                    Overwrite=True
                )
            except Exception:
                raise SSMError(error_message=f'Error saving parameter {ssm_name} in SSM')
        except Exception:
            raise PayloadResponseError()
