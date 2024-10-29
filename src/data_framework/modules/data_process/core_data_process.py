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
