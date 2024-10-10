import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

class Enviroment(StrEnum):
    LOCAL = "local"
    REMOTE = "remote"

class DateLocated(StrEnum):
    FILENAME = "filename"
    COLUMN = "column"

@dataclass
class CSVSpecs:
    skip_rows: int
    infer_schema: bool
    header: bool
    encoding: str
    delimiter: str
    date_located: DateLocated

@dataclass
class DateLocatedFilename:
	regex: str

class LandingFileTypes(StrEnum):
	CSV = "csv"
	JSON = "json"
	EXCEL = "xls"
	
@dataclass
class Payload:
	dataflow: str
	source_file_path: str
	bucket_prefix: str
	file_name: str
	file_date: str

@dataclass
class IncomingFileLandingToRaw:
	zipped: Optional[str] = None
	file_type: LandingFileTypes

@dataclass
class LandingToRaw:
	incoming_file: IncomingFileLandingToRaw

@dataclass
class Flows:
	landing_to_raw: LandingToRaw
	
@dataclass
class Config:
	flows: Flows
	environment: Enviroment
	payload: Payload