from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse
)
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import Technologies
from data_framework.modules.config.model.flows import (
    DatabaseTable
)
from typing import List, Any


class CoreDataProcess(object):

    @LazyClassProperty
    def _data_process(cls) -> DataProcessInterface:
        technology = config().current_process_config().processing_specifications.technology
        if technology == Technologies.EMR:
            from data_framework.modules.data_process.integrations.spark.spark_data_process import SparkDataProcess

            return SparkDataProcess()
        elif technology == Technologies.LAMBDA:
            # TODO: pandas integration
            logger.error(
                'Lambda technology not implemented yet in data framework. ' +
                'Please choose EMR technology in your config file'
            )

    @classmethod
    def merge(cls, dataframe: Any, table_config: DatabaseTable, custom_strategy: str = None) -> WriteResponse:
        response = cls._data_process.merge(
            dataframe=dataframe,
            table_config=table_config,
            custom_strategy=custom_strategy
        )

        if not response.success:
            logger.error(response.error)
            raise response.error

        return response

    @classmethod
    def datacast(
        cls,
        table_source: DatabaseTable,
        table_target: DatabaseTable
    ) -> ReadResponse:
        return cls._data_process.datacast(
            table_source=table_source,
            table_target=table_target
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
    def join(
        cls,
        df_1: Any,
        df_2: Any,
        how: str,
        left_on: List[str],
        right_on: List[str] = None,
        left_suffix: str = '_df_1',
        right_suffix: str = '_df_2'
    ) -> ReadResponse:
        return cls._data_process.join(
            df_1=df_1,
            df_2=df_2,
            how=how,
            left_on=left_on,
            right_on=right_on,
            left_suffix=left_suffix,
            right_suffix=right_suffix
        )

    @classmethod
    def create_dataframe(cls, data: Any, schema: dict = None) -> ReadResponse:
        return cls._data_process.create_dataframe(data=data, schema=schema)

    @classmethod
    def query(cls, sql: str) -> ReadResponse:
        return cls._data_process.query(sql=sql)

    @classmethod
    def overwrite_columns(
        cls,
        dataframe: Any,
        columns: List[str],
        custom_column_suffix: str,
        default_column_suffix: str,
        drop_columns: bool = True
    ) -> ReadResponse:
        return cls._data_process.overwrite_columns(
            dataframe=dataframe,
            columns=columns,
            custom_column_suffix=custom_column_suffix,
            default_column_suffix=default_column_suffix,
            drop_columns=drop_columns
        )

    @classmethod
    def unfold_string_values(cls, dataframe: Any, column_name: str, separator: str) -> ReadResponse:
        return cls._data_process.unfold_string_values(
            dataframe=dataframe, column_name=column_name, separator=separator
        )

    @classmethod
    def add_dynamic_column(
        cls,
        dataframe: Any,
        new_column: str,
        reference_column: str,
        available_columns: List[str],
        default_value: Any = None
    ) -> ReadResponse:
        return cls._data_process.add_dynamic_column(
            dataframe=dataframe,
            new_column=new_column,
            reference_column=reference_column,
            available_columns=available_columns,
            default_value=default_value
        )

    @classmethod
    def stack_columns(
        cls,
        dataframe: Any,
        source_columns: List[str],
        target_columns: List[str]
    ) -> ReadResponse:
        return cls._data_process.stack_columns(
            dataframe=dataframe,
            source_columns=source_columns,
            target_columns=target_columns
        )
