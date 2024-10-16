from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Enviroment(Enum):
    LOCAL = "local"
    REMOTE = "remote"


class DateLocated(Enum):
    FILENAME = "filename"
    COLUMN = "column"


class ColumnParser(Enum):
    DEFAULT = "default"


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
    ordered_columns: bool
    parse_columns: ColumnParser


class LandingFileFormat(Enum):
    CSV = "csv"
    JSON = "json"
    EXCEL = "xls"


@dataclass
class Parameters:
    dataflow: str
    source_file_path: str
    bucket_prefix: str
    file_name: str
    file_date: str
    region: str


@dataclass
class Validations:
    validate_extension: bool
    validate_filename: bool
    validate_csv: bool
    validate_columns: bool


@dataclass
class IncomingFileLandingToRaw:
    zipped: str
    file_format: LandingFileFormat
    filename_pattern: str
    csv_specs: CSVSpecs
    validations: Validations


@dataclass
class Partitions:
    datadate: bool
    insert_time: bool


@dataclass
class OutputFile:
    database: str
    database_relation: str
    table: str
    partitions: Partitions


@dataclass
class LandingToRaw:
    incoming_file: IncomingFileLandingToRaw
    output_file: OutputFile


@dataclass
class Flows:
    landing_to_raw: LandingToRaw


@dataclass
class Config:
    flows: Flows
    environment: Enviroment
    parameters: Parameters
