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


class Database(Enum):
    # TODO: parametrizar dinámicamente
    FUNDS_RAW = "funds_raw"
    FUNDS_STAGING = "funds_staging"
    FUNDS_COMMON = "funds_common"
    FUNDS_BUSINESS = "funds_business"


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


class CoreStorageInterface(ABC):

    @abstractmethod
    def read(self, layer: Layer, key_path: str) -> ReadResponse:
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
    def list_files(self, layer: Layer, prefix: str) -> ListResponse:
        pass
