from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class DataProcessInterface(ABC):

    @abstractmethod
    def merge(self):
        pass

    @abstractmethod
    def datacast(
        database_source: str,
        table_source: str,
        where_source: str,
        database_target: str,
        table_target: str
    ) -> DataFrame:
        pass
