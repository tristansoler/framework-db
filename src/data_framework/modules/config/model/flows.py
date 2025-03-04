from data_framework.modules.storage.interface_storage import Database
from data_framework.modules.notification.interface_notifications import (
    NotificationDict,
    DataFrameworkNotifications
)
from data_framework.modules.exception.config_exceptions import (
    EmptyProcessConfigError,
    ProcessNotFoundError,
    TableKeyError,
    TableConfigNotFoundError
)
from data_framework.modules.exception.data_process_exceptions import TransformationNotImplementedError
from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Optional, List, Tuple, Union
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
    XML = "xml"

class OutputFileFormat(Enum):
    CSV = "csv"
    JSON = "json"

class ExecutionMode(Enum):
    DELTA = "delta"
    FULL = "full"

class JSONSpectFormat(Enum):
    LINES = "lines"
    COLUMNS = "columns"

class JSONSourceLevelFormat(Enum):
    DICTIONARY = "dictionary"
    ARRAY = "array"

class CastingStrategy(Enum):
    ONE_BY_ONE = "one_by_one"
    DYNAMIC = "dynamic"

class TransformationType(Enum):
    PARSE_DATES = "parse_dates"

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
class SparkConfiguration:
    full_volumetric_expectation: VolumetricExpectation = field(default_factory=VolumetricExpectation)
    delta_volumetric_expectation: VolumetricExpectation = field(default_factory=VolumetricExpectation)
    delta_custom: Optional[dict] = field(default_factory=dict)
    full_custom: Optional[dict] = field(default_factory=dict)

    @property
    def volumetric_expectation(self) -> VolumetricExpectation:

        from data_framework.modules.config.core import config

        if config().parameters.execution_mode == ExecutionMode.FULL:
            return self.full_volumetric_expectation

        return self.delta_volumetric_expectation
    
    @property
    def custom_config(self) -> dict:
        
        from data_framework.modules.config.core import config

        if config().parameters.execution_mode == ExecutionMode.FULL:
            return self.full_custom
        
        return self.delta_custom
    
    @property
    def config(self) -> dict:

        from data_framework.modules.config.core import config

        if (config().parameters.execution_mode == ExecutionMode.FULL and self.full_custom
            or config().parameters.execution_mode == ExecutionMode.DELTA and self.delta_custom):
            return self.custom_config
        else:

            from data_framework.modules.data_process.integrations.spark.dynamic_config import DynamicConfig

            volumetric = self.volumetric_expectation
            return DynamicConfig.recommend_spark_config(
                    dataset_size_gb=volumetric.data_size_gb,
                    avg_file_size_mb=volumetric.avg_file_size_mb
                )

@dataclass
class ProcessingSpecifications:
    technology: Technologies = Technologies.EMR
    spark_configuration: SparkConfiguration = field(default_factory=SparkConfiguration)


@dataclass
class DateLocatedFilename:
    regex: str


class InterfaceSpecs:

    @property
    def read_config(self) -> dict:
        raise NotImplementedError('It is mandatory to implement read_config property')
    
    @property
    def pandas_read_config(self) -> dict:
        raise NotImplementedError('It is mandatory to implement pandas_read_config property')
    
@dataclass
class XMLSpecs(InterfaceSpecs):
    encoding: str
    xpath: str
    date_located: DateLocated
    date_located_filename: DateLocatedFilename

    @property
    def read_config(self) -> dict:
        return {}
    
    @property
    def pandas_read_config(self) -> dict:
        return {}


@dataclass
class JSONSpecs(InterfaceSpecs):
    encoding: str
    source_level: Optional[str]
    
    date_located: DateLocated
    date_located_filename: DateLocatedFilename
    format: Optional[JSONSpectFormat]
    values_to_string: bool = False
    source_level_format: JSONSourceLevelFormat = JSONSourceLevelFormat.ARRAY

    @property
    def read_config(self) -> dict:
        return {}
    
    @property
    def pandas_read_config(self) -> dict:
        config = {}

        if self.values_to_string:
            config["dtype"] = str

        if self.format == JSONSpectFormat.LINES:
            config["lines"] = True
            config["orient"] = "records"
        elif self.format == JSONSpectFormat.COLUMNS:
            config["orient"] = "columns"

        return config

    @property
    def levels(self) -> list[str]:
        return self.source_level.split('.')

@dataclass
class CSVSpecs(InterfaceSpecs):
    header_position: int
    header: bool
    encoding: str
    delimiter: str
    date_located: DateLocated
    date_located_filename: DateLocatedFilename
    escape: Optional[str] = None
    comment: Optional[str] = None
    null_value: Optional[str] = None
    nan_value: Optional[str] = None
    special_character: Optional[str] = None
    multiline: bool = False

    @property
    def read_config(self) -> dict:
        config = {
            "header": str(self.header).lower(),
            "encoding": self.encoding,
            "sep": self.delimiter,
            "multiLine": str(self.multiline).lower()
        }
        if self.special_character:
            config["quote"] = self.special_character
        if self.escape:
            config["escape"] = self.escape
        if self.comment:
            config["comment"] = self.comment
        if self.null_value:
            config["nullValue"] = self.null_value
        if self.nan_value:
            config["nanValue"] = self.nan_value
        return config
    

    @property
    def pandas_read_config(self) -> dict:
        return {}

@dataclass
class CSVSpecsReport:
    header: bool
    index: bool
    encoding: str
    delimiter: str


@dataclass
class JSONSpecsReport:
    format: JSONSpectFormat = JSONSpectFormat.LINES


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
    csv_specs: Optional[CSVSpecs]
    xml_specs: Optional[XMLSpecs]
    json_specs: Optional[JSONSpecs]
    compare_with_previous_file: Optional[bool] = False

    @property
    def specifications(self) -> Union[CSVSpecs, XMLSpecs, JSONSpecs]:
        if self.file_format == LandingFileFormat.XML:
            return self.xml_specs
        elif self.file_format == LandingFileFormat.JSON:
            return self.json_specs
        else:
            return self.csv_specs


@dataclass
class Transformation:
    type: TransformationType

    @classmethod
    def get_subclass_from_dict(cls, transformation: dict):
        transformation_type = transformation.get('type')
        transformation_mapping = {
            TransformationType.PARSE_DATES.value: ParseDatesTransformation
        }
        subclass = transformation_mapping.get(transformation_type)
        if not subclass:
            raise TransformationNotImplementedError(
                transformation=transformation_type,
                available_types=list(transformation_mapping.keys())
            )
        return subclass


@dataclass
class ParseDatesTransformation(Transformation):
    columns: List[str]
    source_format: List[str]
    target_format: str = "yyyy-MM-dd"


@dataclass
class Casting:
    strategy: CastingStrategy = CastingStrategy.ONE_BY_ONE
    master_table: Optional[str] = None
    transformations: List[Transformation] = field(default_factory=list)


@dataclass
class DatabaseTable:
    database: Database
    table: str
    primary_keys: Optional[list] = field(default_factory=list)
    casting: Casting = field(default_factory=Casting)
    partition_field: str = "data_date"

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
            raise TableKeyError(
                table_key=table_key,
                available_table_keys=list(self.tables.keys())
            )
        return table_info

    def table_key(self, database: str, table: str) -> Union[str, None]:
        for table_key, database_table in self.tables.items():
            if database_table.database.value == database and database_table.table == table:
                return table_key
        raise TableConfigNotFoundError(database=database, table=table)


@dataclass
class LandingToRaw:
    incoming_file: IncomingFileLandingToRaw
    output_file: DatabaseTable
    processing_specifications: ProcessingSpecifications = field(default_factory=ProcessingSpecifications)
    notifications: NotificationDict = field(default_factory=NotificationDict)


@dataclass
class ProcessVars:
    _variables: dict = field(default_factory=dict)

    def get_variable(self, name: str):
        return self._variables.get(name)


@dataclass
class GenericProcess:
    source_tables: TableDict
    target_tables: TableDict
    processing_specifications: ProcessingSpecifications = field(default_factory=ProcessingSpecifications)
    notifications: NotificationDict = field(default_factory=NotificationDict)
    vars: Optional[ProcessVars] = field(default_factory=ProcessVars)


@dataclass
class OutputReport:
    name: str
    source_table: DatabaseTable
    columns: List[str]
    file_format: OutputFileFormat
    filename_pattern: str
    csv_specs: Optional[CSVSpecsReport]
    json_specs: Optional[JSONSpecsReport]
    replaces: Optional[List[dict]]
    description: Optional[str]
    where: Optional[str]
    columns_alias: Optional[List[str]] = field(default_factory=list)
    filename_date_format: Optional[str] = '%Y-%m-%d'


@dataclass
class ToOutput:
    output_reports: List[OutputReport]
    processing_specifications: ProcessingSpecifications
    notifications: NotificationDict = field(default_factory=NotificationDict)


@dataclass
class Processes:
    landing_to_raw: Optional[LandingToRaw]
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
    data_framework_notifications: DataFrameworkNotifications

    @property
    def has_next_process(self) -> bool:
        processes = [field.name for field in fields(Processes)]
        current_process_index = processes.index(self.parameters.process)
        posible_processes_begind_index = current_process_index + 1

        possible_processes = processes[posible_processes_begind_index:]
        next_processes = [process for process in possible_processes if getattr(self.processes, process) is not None]

        return not next_processes

    @property
    def is_first_process(self) -> bool:
        procesess = [field.name for field in fields(Processes)]
        next_processes = [process for process in procesess if getattr(self.processes, process) is not None]
        current_process_index = next_processes.index(self.parameters.process)

        return current_process_index == 0

    def current_process_config(self) -> Union[LandingToRaw, GenericProcess, ToOutput]:
        try:
            current_process = self.parameters.process
            current_process_config = getattr(self.processes, current_process)
            if current_process_config is None:
                raise EmptyProcessConfigError(process=current_process)
            return current_process_config
        except AttributeError:
            raise ProcessNotFoundError(
                process=current_process,
                available_processes=list(self.processes.__dict__.keys())
            )
