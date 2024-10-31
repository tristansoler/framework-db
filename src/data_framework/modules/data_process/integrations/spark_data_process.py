from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse
)
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.data_process.helpers.cast import Cast
from typing import List
from pyspark.sql import SparkSession, DataFrame


class SparkDataProcess(DataProcessInterface):

    def __init__(self):
        # Obtain Spark configuration for the current process
        process = config().parameters.process
        spark_config = getattr(config().processes, process) \
            .processing_specifications.spark_configuration
        # Initialize Spark session
        spark_session = SparkSession.builder
        # Configure Iceberg catalog
        if spark_config.default_catalog:
            spark_session = spark_session.config(
                "spark.sql.catalog.iceberg_catalog",
                "org.apache.iceberg.spark.SparkCatalog"
            )
        # Configure Iceberg warehouse
        spark_session = spark_session.config(
            "spark.sql.catalog.iceberg_catalog.warehouse",
            f"{spark_config.warehouse}/"
        )
        # Add custom configurations
        for custom_config in spark_config.custom_configuration:
            spark_session = spark_session.config(
                custom_config['parameter'],
                custom_config['value']
            )
        # Create Spark session
        spark_session = spark_session.enableHiveSupport().getOrCreate()
        self.spark = spark_session
        self.catalog = 'iceberg_catalog'

    def _build_complete_table_name(self, database: str, table: str) -> str:
        return f'{self.catalog}.{database}.{table}'

    def _build_simple_table_name(self, database: str, table: str) -> str:
        return f'{database}.{table}'

    def merge(self, df: DataFrame, database: str, table: str, primary_keys: List[str]) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(database, table)
            view_name = 'data_to_merge'
            df.createOrReplaceTempView(view_name)
            sql_update_with_pks = '\n'.join([
                f'AND {view_name}.{field} = {table_name}.{field}' for field in primary_keys
            ])
            merge_query = f"""
                MERGE INTO {table_name}
                USING {view_name} ON
                    {sql_update_with_pks}
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """
            self._execute_query(merge_query)
            response = WriteResponse(success=True, error=None)
        except Exception as e:
            response = WriteResponse(success=False, error=e)
        return response

    def datacast(
        self,
        database_source: str,
        table_source: str,
        database_target: str,
        table_target: str,
        partition_field: str = None,
        partition_value: str = None
    ) -> ReadResponse:
        try:
            cast = Cast()
            query = cast.get_query_datacast(
                database_source,
                table_source,
                database_target,
                table_target,
                partition_field,
                partition_value
            )
            df = self._execute_query(query)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def _execute_query(self, query: str) -> DataFrame:
        df_result = self.spark.sql(query)
        return df_result

    def read_table(self, database: str, table: str) -> ReadResponse:
        try:
            table_name = self._build_simple_table_name(database, table)
            query = f"SELECT * FROM {table_name}"
            df = self._execute_query(query)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def read_table_with_filter(self, database: str, table: str, _filter: str) -> ReadResponse:
        try:
            table_name = self._build_simple_table_name(database, table)
            query = f"SELECT * FROM {table_name} WHERE {_filter}"
            df = self._execute_query(query)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def join(self, df_1: DataFrame, df_2: DataFrame, on: List[str], how: str) -> ReadResponse:
        try:
            if how not in ['inner', 'left', 'right', 'outer']:
                raise ValueError(
                    f'Invalid parameter value: how={how}. Allowed values: inner, left, right, outer'
                )
            # TODO: join por columnas diferentes en cada df -> similar a left_on y right_on en pandas
            df_result = df_1.join(df_2, on=on, how=how)
            # TODO: revisar tipo de respuesta. ¿TransformationResponse?
            response = ReadResponse(success=True, error=None, data=df_result)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response
