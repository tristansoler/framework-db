from data_framework.modules.storage.interface_storage import Database
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Tuple, Union, Dict
import os


class Environment(Enum):
    LOCAL = "local"
    DEVELOP = "develop"
    PREPRODUCTION = "preproduction"
    PRODUCTION = "production"


class Platform(Enum):
    # TODO: remove when migrating Infinity to Data Platform
    DATA_PLATFORM = "data_platform"
    INFINITY = "infinity"


class DateLocated(Enum):
    FILENAME = "filename"
    COLUMN = "column"


class Technologies(Enum):
    LAMBDA = "lambda"
    EMR = "emr"


class LandingFileFormat(Enum):
    CSV = "csv"
    JSON = "json"
    EXCEL = "xls"


class ExecutionMode(Enum):
    DELTA = "delta"
    FULL = "full"


class NotificationType(Enum):
    EMAIL = "email"


class Topic(Enum):
    INTERNAL = "internal"
    EXTERNAL = "external"


@dataclass
class Hardware:
    ram: int = 4
    cores: int = 2
    disk: int = 20


@dataclass
class VolumetricExpectation:
    data_size_gb: float = 0.1
    avg_file_size_mb: int = 100


@dataclass
class CustomConfiguration:
    parameter: str
    value: str


@dataclass
class SparkConfiguration:
    full_volumetric_expectation: VolumetricExpectation = field(default_factory=VolumetricExpectation)
    delta_volumetric_expectation: VolumetricExpectation = field(default_factory=VolumetricExpectation)
    custom_configuration: List[CustomConfiguration] = field(default_factory=list)

    @property
    def volumetric_expectation(self) -> VolumetricExpectation:
        from data_framework.modules.config.core import config
        if config().parameters.execution_mode == ExecutionMode.FULL:
            return self.full_volumetric_expectation

        return self.delta_volumetric_expectation


@dataclass
class ProcessingSpecifications:
    technology: Technologies
    spark_configuration: Optional[SparkConfiguration] = None


@dataclass
class DateLocatedFilename:
    regex: str


@dataclass
class CSVSpecs:
    header_position: int
    header: bool
    encoding: str
    delimiter: str
    date_located: DateLocated
    date_located_filename: DateLocatedFilename
    special_character: Optional[str] = None

    def read_config(self) -> dict:
        config = {
            "header": str(self.header).lower(),
            "encoding": self.encoding,
            "sep": self.delimiter
        }

        if self.special_character:
            config["quote"] = self.special_character

        return config


@dataclass
class CSVSpecsReport:
    header: bool
    index: bool
    encoding: str
    delimiter: str


@dataclass
class Parameters:
    dataflow: str
    process: str
    table: str
    source_file_path: str
    bucket_prefix: str
    file_name: Optional[str]
    file_date: Optional[str]
    execution_mode: ExecutionMode = ExecutionMode.DELTA

    @property
    def region(self) -> str:
        return os.environ["AWS_REGION"]


@dataclass
class IncomingFileLandingToRaw:
    zipped: Optional[str]
    file_format: LandingFileFormat
    filename_pattern: str
    filename_unzipped_pattern: Optional[str]
    csv_specs: CSVSpecs
    compare_with_previous_file: Optional[bool] = False


@dataclass
class Notification:
    type: NotificationType
    topics: List[Topic]
    subject: str
    body: str


@dataclass
class NotificationDict:
    notifications: Dict[str, Notification] = field(default_factory=dict)

    def get_notification(self, notification_name: str) -> Union[Notification, None]:
        if not self.notifications:
            raise ValueError(f'Notification key {notification_name} not found. No notifications defined')
        notification = self.notifications.get(notification_name)
        if not notification:
            notification_names = ', '.join(self.notifications.keys())
            raise ValueError(
                f'Notification key {notification_name} not found. Available notification keys: {notification_names}'
            )
        return notification


@dataclass
class DatabaseTable:
    database: Database
    table: str
    primary_keys: Optional[list] = field(default_factory=list)
    partition_field: str = "datadate"  # TODO: data_date

    @property
    def database_relation(self) -> str:
        from data_framework.modules.config.core import config
        if config().platform == Platform.INFINITY:
            return self.database.value
        else:
            return f'rl_{self.database.value}'

    @property
    def full_name(self) -> str:
        return f'{self.database_relation}.{self.table}'

    @property
    def sql_where(self) -> str:
        from data_framework.modules.config.core import config

        if config().parameters.execution_mode == ExecutionMode.DELTA:
            return f"{self.partition_field} = '{config().parameters.file_date}'"
        return ""


@dataclass
class TableDict:
    tables: Tuple[str, DatabaseTable]

    def table(self, table_key: str) -> Union[DatabaseTable, None]:
        table_info = self.tables.get(table_key)
        if not table_info:
            table_keys = ', '.join(self.tables.keys())
            raise ValueError(
                f'Table key {table_key} not found. Available table keys: {table_keys}'
            )
        return table_info

    def table_key(self, database: str, table: str) -> Union[str, None]:
        for table_key, database_table in self.tables.items():
            if database_table.database.value == database and database_table.table == table:
                return table_key
        raise ValueError(f'Table key for {database}.{table} not found in config file')


@dataclass
class LandingToRaw:
    incoming_file: IncomingFileLandingToRaw
    output_file: DatabaseTable
    processing_specifications: ProcessingSpecifications
    notifications: NotificationDict = field(default_factory=NotificationDict)


@dataclass
class GenericProcess:
    source_tables: TableDict
    target_tables: TableDict
    processing_specifications: ProcessingSpecifications
    notifications: NotificationDict = field(default_factory=NotificationDict)


@dataclass
class OutputReport:
    name: str
    source_table: DatabaseTable
    columns: List[str]
    where: str
    file_format: str
    filename_pattern: str
    csv_specs: CSVSpecsReport
    description: Optional[str]
    columns_alias: Optional[List[str]] = field(default_factory=list)
    filename_date_format: Optional[str] = '%Y-%m-%d'


@dataclass
class ToOutput:
    output_reports: List[OutputReport]
    processing_specifications: ProcessingSpecifications
    notifications: NotificationDict = field(default_factory=NotificationDict)


@dataclass
class Processes:
    landing_to_raw: LandingToRaw
    raw_to_staging: Optional[GenericProcess] = None
    staging_to_common: Optional[GenericProcess] = None
    staging_to_business: Optional[GenericProcess] = None
    common_to_business: Optional[GenericProcess] = None
    common_to_output: Optional[ToOutput] = None
    business_to_output: Optional[ToOutput] = None


@dataclass
class Config:
    processes: Processes
    environment: Environment
    # TODO: remove when migrating Infinity to Data Platform
    platform: Platform
    parameters: Parameters
    project_id: str

    def current_process_config(self) -> Union[LandingToRaw, GenericProcess, ToOutput]:
        try:
            current_process = self.parameters.process
            current_process_config = getattr(self.processes, current_process)
            if current_process_config is None:
                raise ValueError(f'Configuration of process {current_process} is empty')
            return current_process_config
        except AttributeError:
            processes = ', '.join(self.processes.__dict__.keys())
            raise ValueError(
                f'Process {current_process} not found in config. Available processes: {processes}'
            )
