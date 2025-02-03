from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass


class Layer(Enum):
    LANDING = "landing"
    RAW = "raw"
    STAGING = "staging"
    COMMON = "common"
    BUSINESS = "business"
    OUTPUT = "output"
    ATHENA = "athena"
    # TODO: bucket for Athena in Infinity. Remove when migrating to Data Platform
    TEMP = "temp"


class Database(Enum):
    FUNDS_RAW = "funds_raw"
    FUNDS_STAGING = "funds_staging"
    FUNDS_COMMON = "funds_common"
    FUNDS_BUSINESS = "funds_business"
    DATA_QUALITY = "funds_common"
    CONFIG_SCHEMAS = 'df_config_schemas'
    # TODO: DBs for Infinity quality controls. Remove when migrating to Data Platform
    INFINITY_STAGING = "infinity_datalake_staging"
    INFINITY_COMMON = "infinity_datalake_common"
    INFINITY_BUSINESS = "infinity_datalake_business"
    SAM_STG = "sam_stg"
    SAM_DWH = "sam_dwh"


@dataclass
class ReadResponse:
    success: bool
    error: str
    data: bytes


@dataclass
class WriteResponse:
    success: bool
    error: str


@dataclass
class ListResponse:
    success: bool
    error: str
    result: list

@dataclass
class PathResponse:
    success: bool
    error: str
    path: str


class CoreStorageInterface(ABC):

    @abstractmethod
    def read(self, layer: Layer, key_path: str, bucket: str = None) -> ReadResponse:
        pass

    @abstractmethod
    def write(
        self,
        layer: Layer,
        database: Database,
        table: str,
        data: bytes,
        partitions: str,
        filename: str
    ) -> WriteResponse:
        pass

    @abstractmethod
    def write_to_path(self, layer: Layer, key_path: str, data: bytes) -> WriteResponse:
        pass

    @abstractmethod
    def list_files(self, layer: Layer, prefix: str) -> ListResponse:
        pass

    @abstractmethod
    def raw_layer_path(self, database: Database, table_name: str) -> PathResponse:
        pass
    
    @abstractmethod
    def base_layer_path(self, layer: Layer) -> PathResponse:
        pass
