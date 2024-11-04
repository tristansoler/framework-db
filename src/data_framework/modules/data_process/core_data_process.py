from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse
)
from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Technologies
from typing import List, Any


class CoreDataProcess(object):

    @LazyClassProperty
    def _data_process(cls) -> DataProcessInterface:
        process = config().parameters.process
        technology = getattr(config().processes, process) \
            .processing_specifications.technology
        if technology == Technologies.EMR.value:
            from data_framework.modules.data_process.integrations.spark_data_process import SparkDataProcess
            return SparkDataProcess()
        elif technology == Technologies.LAMBDA.value:
            # TODO: pandas integration
            pass

    @classmethod
    def merge(cls, df: Any, database: str, table: str, primary_keys: List[str]) -> WriteResponse:
        return cls._data_process.merge(
            df=df,
            database=database,
            table=table,
            primary_keys=primary_keys
        )

    @classmethod
    def datacast(
        cls,
        database_source: str,
        table_source: str,
        database_target: str,
        table_target: str,
        partition_field: str = None,
        partition_value: str = None
    ) -> ReadResponse:
        return cls._data_process.datacast(
            database_source=database_source,
            table_source=table_source,
            database_target=database_target,
            table_target=table_target,
            partition_field=partition_field,
            partition_value=partition_value
        )

    @classmethod
    def read_table(cls, database: str, table: str) -> ReadResponse:
        return cls._data_process.read_table(database=database, table=table)

    @classmethod
    def read_table_with_filter(cls, database: str, table: str, _filter: str) -> ReadResponse:
        return cls._data_process.read_table_with_filter(
            database=database, table=table, _filter=_filter
        )

    @classmethod
    def join(cls, df_1: Any, df_2: Any, on: List[str], how: str) -> ReadResponse:
        return cls._data_process.join(df_1=df_1, df_2=df_2, on=on, how=how)

    @classmethod
    def create_dataframe(cls, schema: dict, rows: List[dict]) -> ReadResponse:
        return cls._data_process.create_dataframe(schema=schema, rows=rows)

    @classmethod
    def append_rows_to_dataframe(cls, df: Any, new_rows: List[dict]) -> ReadResponse:
        return cls._data_process.append_rows_to_dataframe(df=df, new_rows=new_rows)
