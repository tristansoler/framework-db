from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse
)
from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Technologies
from data_framework.modules.config.model.flows import (
    DatabaseTable
)
from typing import List, Any


class CoreDataProcess(object):

    @LazyClassProperty
    def _data_process(cls) -> DataProcessInterface:
        technology = config().current_process_config().processing_specifications.technology
        if technology == Technologies.EMR.value:
            from data_framework.modules.data_process.integrations.spark_data_process import SparkDataProcess
            return SparkDataProcess()
        elif technology == Technologies.LAMBDA.value:
            # TODO: pandas integration
            pass

    @classmethod
    def merge(cls, dataframe: Any, table_config: DatabaseTable) -> WriteResponse:
        return cls._data_process.merge(
            dataframe=dataframe,
            table_config=table_config
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
    def read_table(cls, database: str, table: str, filter: str = None, columns: List[str] = None) -> ReadResponse:
        return cls._data_process.read_table(database=database, table=table, filter=filter, columns=columns)

    @classmethod
    def delete_from_table(cls, table_config: DatabaseTable, _filter: str) -> WriteResponse:
        return cls._data_process.delete_from_table(table_config=table_config, _filter=_filter)

    @classmethod
    def insert_dataframe(cls, dataframe: Any, table_config: DatabaseTable) -> WriteResponse:
        return cls._data_process.insert_dataframe(dataframe=dataframe, table_config=table_config)

    @classmethod
    def join(cls, df_1: Any, df_2: Any, on: List[str], how: str) -> ReadResponse:
        return cls._data_process.join(df_1=df_1, df_2=df_2, on=on, how=how)

    @classmethod
    def create_dataframe(cls, schema: dict, rows: List[dict]) -> ReadResponse:
        return cls._data_process.create_dataframe(schema=schema, rows=rows)

    @classmethod
    def append_rows_to_dataframe(cls, df: Any, new_rows: List[dict]) -> ReadResponse:
        return cls._data_process.append_rows_to_dataframe(df=df, new_rows=new_rows)

    @classmethod
    def query(cls, sql: str) -> ReadResponse:
        return cls._data_process.query(query=sql)
