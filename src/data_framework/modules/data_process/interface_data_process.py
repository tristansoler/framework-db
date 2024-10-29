from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Any


@dataclass
class ReadResponse:
    success: bool
    error: str
    data: Any


@dataclass
class WriteResponse:
    success: bool
    error: str


class DataProcessInterface(ABC):

    @abstractmethod
    def merge(self, df: Any, database: str, table: str, primary_keys: List[str]) -> WriteResponse:
        pass

    @abstractmethod
    def datacast(
        self,
        database_source: str,
        table_source: str,
        database_target: str,
        table_target: str,
        partition_field: str = None,
        partition_value: str = None
    ) -> ReadResponse:
        pass
