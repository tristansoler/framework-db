from data_framework.modules.storage.interface_storage import Database
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Tuple, Union
import os

class Environment(Enum):
    LOCAL = "local"
    REMOTE = "remote"


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


@dataclass
class Hardware:
    ram: int
    cpu: Optional[int]
    disk: Optional[int]


@dataclass
class CustomConfiguration:
    parameter: str
    value: str


@dataclass
class SparkConfiguration:
    catalog: str
    warehouse: Database
    custom_configuration: List[CustomConfiguration] = field(default_factory=list)


@dataclass
class ProcessingSpecifications:
    technology: Technologies
    hardware: Hardware
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


@dataclass
class CSVSpecsReport:
    header: bool
    encoding: str
    delimiter: str


@dataclass
class Parameters:
    dataflow: str
    process: str
    table: str
    source_file_path: str
    file_name: str
    file_date: Optional[str]

    @property
    def region(self) -> str:
        return os.environ["AWS_REGION"]
    
    @property
    def bucket_prefix(self) -> str:
        return "aihd1airas3aihgdp"

@dataclass
class Validations:
    validate_extension: bool
    validate_filename: bool
    validate_csv: bool
    validate_columns: bool


@dataclass
class IncomingFileLandingToRaw:
    zipped: Optional[str]
    file_format: LandingFileFormat
    filename_pattern: str
    filename_unzipped_pattern: Optional[str]
    csv_specs: CSVSpecs
    validations: Validations


@dataclass
class DatabaseTable:
    database: Database
    table: str
    primary_keys: Optional[list]
    partition_field: str = "datadate"

    @property
    def database_relation(self) -> str:
        return f'rl_{self.database}'

    @property
    def full_name(self) -> str:
        return f'{self.database_relation}.{self.table}'
    
    @property
    def sql_where(self) -> str:
        from data_framework.modules.config.core import config
        return f"{self.partition_field} = '{config().parameters.file_date}'"


@dataclass
class TableDict:
    tables: Tuple[str, DatabaseTable]

    def table(self, table_key: str) -> Union[DatabaseTable, None]:
        table_info = self.tables.get(table_key)
        if not table_info:
            raise ValueError(
                f'Table key {table_key} not found. Available table keys: {list(self.tables.keys())}'
            )
        return table_info


@dataclass
class LandingToRaw:
    incoming_file: IncomingFileLandingToRaw
    output_file: DatabaseTable
    processing_specifications: ProcessingSpecifications


@dataclass
class GenericProcess:
    source_tables: TableDict
    target_tables: TableDict
    processing_specifications: ProcessingSpecifications


@dataclass
class OutputReport:
    name: Database
    table: str
    columns: str
    columns_alias: str
    where: str
    file_format: str
    filename_pattern: str
    csv_specs: CSVSpecsReport


@dataclass
class ToOutput:
    output_reports: List[OutputReport]
    processing_specifications: ProcessingSpecifications


@dataclass
class Processes:
    landing_to_raw: LandingToRaw
    raw_to_staging: Optional[GenericProcess] = None
    staging_to_common: Optional[GenericProcess] = None
    staging_to_business: Optional[GenericProcess] = None
    to_output: Optional[ToOutput] = None


@dataclass
class Config:
    processes: Processes
    environment: Environment
    parameters: Parameters
