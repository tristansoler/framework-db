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
    FUNDS = "funds"

@dataclass
class ReadResponse:
    success: bool
    error: str
    data: bytes

@dataclass
class WriteResponse:
    success: bool
    error: str

class CoreStorageInterface(ABC):

    @abstractmethod
    def read(self, layer: Layer, database: Database, table: str) -> ReadResponse:
        pass

    @abstractmethod
    def write(self, layer: Layer, database: Database, table: str, data: bytes, partitions: str) -> WriteResponse:
        pass