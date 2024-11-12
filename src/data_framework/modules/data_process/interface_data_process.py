from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Any
from data_framework.modules.config.model.flows import (
    DatabaseTable
)

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
    def merge(self, dataframe: Any, table_config: DatabaseTable) -> WriteResponse:
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

    @abstractmethod
    def read_table(self, database: str, table: str, filter: str, columns: List[str]) -> ReadResponse:
        pass

    @abstractmethod
    def join(self, df_1: Any, df_2: Any, on: List[str], how: str) -> ReadResponse:
        pass

    @abstractmethod
    def create_dataframe(self, schema: dict, rows: List[dict]) -> ReadResponse:
        pass

    @abstractmethod
    def append_rows_to_dataframe(self, df: Any, new_rows: List[dict]) -> ReadResponse:
        pass
