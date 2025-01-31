from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Any
from data_framework.modules.config.model.flows import DatabaseTable


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
    def merge(self, dataframe: Any, table_config: DatabaseTable, custom_strategy: str = None) -> WriteResponse:
        pass

    @abstractmethod
    def datacast(
        self,
        table_source: DatabaseTable,
        table_target: DatabaseTable
    ) -> ReadResponse:
        pass

    @abstractmethod
    def read_table(self, database: str, table: str, filter: str, columns: List[str]) -> ReadResponse:
        pass

    @abstractmethod
    def delete_from_table(self, table_config: DatabaseTable, _filter: str) -> WriteResponse:
        pass

    @abstractmethod
    def insert_dataframe(self, dataframe: Any, table_config: DatabaseTable) -> WriteResponse:
        pass

    @abstractmethod
    def join(
        self,
        df_1: Any,
        df_2: Any,
        how: str,
        left_on: List[str],
        right_on: List[str] = None,
        left_suffix: str = '_df_1',
        right_suffix: str = '_df_2'
    ) -> ReadResponse:
        pass

    @abstractmethod
    def create_dataframe(self, data: Any, schema: str = None) -> ReadResponse:
        pass

    @abstractmethod
    def query(self, sql: str) -> ReadResponse:
        pass

    @abstractmethod
    def overwrite_columns(
        self,
        dataframe: Any,
        columns: List[str],
        custom_column_suffix: str,
        default_column_suffix: str,
        drop_columns: bool = True
    ) -> ReadResponse:
        pass

    @abstractmethod
    def unfold_string_values(self, dataframe: Any, column_name: str, separator: str) -> ReadResponse:
        pass

    @abstractmethod
    def add_dynamic_column(
        self,
        dataframe: Any,
        new_column: str,
        reference_column: str,
        available_columns: List[str],
        default_value: Any = None
    ) -> ReadResponse:
        pass

    @abstractmethod
    def stack_columns(
        self,
        dataframe: Any,
        source_columns: List[str],
        target_columns: List[str]
    ) -> ReadResponse:
        pass

    @abstractmethod
    def is_empty(self, dataframe: Any) -> bool:
        pass

    @abstractmethod
    def count_rows(self, dataframe: Any) -> int:
        pass

    @abstractmethod
    def select_columns(self, dataframe: Any, columns: List[str]) -> ReadResponse:
        pass

    @abstractmethod
    def show_dataframe(self, dataframe: Any) -> WriteResponse:
        pass
