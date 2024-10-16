from abc import ABC, abstractmethod
from enum import Enum

class Layer(Enum):
    LANDING = "landing"
    RAW = "raw"
    STAGING = "staging"
    COMMON = "staging"

class Database(Enum):
    FUND = "funds"

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