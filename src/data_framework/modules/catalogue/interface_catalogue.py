from abc import ABC, abstractmethod
from typing import Union, Optional, List, Dict
from dataclasses import dataclass


@dataclass
class Column:
    name: str
    type: str
    order: int
    ispartitioned: bool


@dataclass
class Schema:
    columns: (Column)

    def get_column_names(self, partitioned: bool = False) -> List[str]:
        if partitioned:
            return [column.name for column in self.columns]
        else:
            return [column.name for column in self.columns if not column.ispartitioned]

    def get_type_columns(self, partitioned: bool = False) -> List[str]:
        if partitioned:
            return [column.type for column in self.columns]
        else:
            return [column.type for column in self.columns if not column.ispartitioned]

    def get_column_type_mapping(self, partitioned: bool = False) -> Dict[str, str]:
        if partitioned:
            return {
                column.name.lower(): column.type
                for column in self.columns
            }
        else:
            return {
                column.name.lower(): column.type
                for column in self.columns
                if not column.ispartitioned
            }


@dataclass
class SchemaResponse:
    success: bool
    error: Optional[str] = None
    schema: Optional[Schema] = None


@dataclass
class GenericResponse:
    success: bool
    error: Optional[str] = None


class CatalogueInterface(ABC):

    @abstractmethod
    def create_partition(
        self,
        database: str,
        table: str,
        artition_field: str,
        partition_value: Union[str, int]
    ) -> GenericResponse:
        # Abstract class to define the basic storage interface
        pass

    @abstractmethod
    def get_schema(self, database: str, table: str) -> SchemaResponse:
        # Abstract method to write data to a specific location
        pass
