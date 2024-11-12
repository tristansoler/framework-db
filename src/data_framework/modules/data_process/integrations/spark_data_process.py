from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse,
)
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.helpers.cast import Cast
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.config.model.flows import (
    DatabaseTable
)
from typing import List
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    DateType,
    FloatType,
    TimestampType,
    DecimalType
)


class SparkDataProcess(DataProcessInterface):

    def __init__(self):
        # Obtain Spark configuration for the current process
        process = config().parameters.process
        json_config = getattr(config().processes, process) \
            .processing_specifications.spark_configuration

        spark_config = SparkConf() \
            .setAppName(f"[{config().parameters.dataflow}] {config().parameters.process}")

        spark_config.setAll([
            # Iceberg
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.jars", "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"),
            ("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
            # Configure Iceberg warehouse
            ("spark.sql.catalog.iceberg_catalog.warehouse", f"{json_config.warehouse}/"),
            # Hive
            ("spark.hadoop.hive.exec.dynamic.partition", "true"),
            ("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict"),
            (
                "spark.hadoop.hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            ),
            # AWS Glue
            ("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
            ("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            ("spark.sql.catalogImplementation", "hive"),

            # Configure hardware
            # TODO: Set dynamic values from config
            ("spark.executor.memory", "4g"),
            ("spark.executor.cores", "1"),
            ("spark.driver.cores", "1"),
            ("spark.driver.memory", "4g"),
            ("spark.executor.instances", "1")
        ])

        # Add custom configurations
        for custom_config in json_config.custom_configuration:
            spark_config.set(custom_config.parameter, custom_config.value)
        # Create Spark session
        self.spark = SparkSession.builder \
            .config(conf=spark_config) \
            .enableHiveSupport() \
            .getOrCreate()

        self.catalogue = CoreCatalogue()

    def _build_complete_table_name(self, database: str, table: str) -> str:
        return f'iceberg_catalog.{database}.{table}'

    def _build_simple_table_name(self, database: str, table: str) -> str:
        return f'{database}.{table}'

    def merge(self, dataframe: DataFrame, table_config: DatabaseTable) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            view_name = 'data_to_merge'
            # Select only necessary columns of the dataframe
            table_schema = self.catalogue.get_schema(database=table_config.database_relation, table=table_config.table)
            table_columns = table_schema.schema.get_column_names(partitioned=True)
            dataframe = dataframe.select(*table_columns)
            # Perform merge
            dataframe.createOrReplaceTempView(view_name)
            sql_update_with_pks = '\n AND '.join([
                f' {view_name}.{field} = {table_name}.{field}' for field in table_config.primary_keys
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

            logger.debug(f'merge sql \n{merge_query}')
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

            logger.debug(
                f"""
                    query of casting
                    {query}
                """
            )

            df = self._execute_query(query)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def _execute_query(self, query: str) -> DataFrame:
        df_result = self.spark.sql(query)
        return df_result

    def read_table(self, database: str, table: str, filter: str = None, columns: List[str] = None) -> ReadResponse:
        try:
            table_name = self._build_simple_table_name(database, table)
            if columns:
                columns_str = ', '.join(columns)
                query = f"SELECT {columns_str} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
            if filter:
                query += f" WHERE {filter}"
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

    def create_dataframe(self, schema: dict, rows: List[dict]) -> ReadResponse:
        try:
            spark_schema = self._parse_schema(schema)
            df_result = self.spark.createDataFrame(rows, spark_schema)
            response = ReadResponse(success=True, error=None, data=df_result)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def _parse_schema(self, schema: dict) -> StructType:
        parsed_types = {
            'int': IntegerType(),
            'string': StringType(),
            'double': DoubleType(),
            'float': FloatType(),
            'decimal': DecimalType(),
            'bool': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType()
        }
        parsed_fields = []
        for field, field_info in schema.items():
            _type = field_info['type']
            is_null = field_info['is_null']
            parsed_type = parsed_types.get(_type)
            if not parsed_type:
                raise ValueError(f'Invalid type: {_type}. Allowed types: {list(parsed_types.keys())}')
            parsed_fields.append(
                StructField(field, parsed_type, is_null)
            )
        spark_schema = StructType(parsed_fields)
        return spark_schema

    def append_rows_to_dataframe(self, df: DataFrame, new_rows: List[dict]) -> ReadResponse:
        try:
            spark_schema = df.schema
            new_df = self.spark.createDataFrame(new_rows, spark_schema)
            df_result = df.unionByName(new_df)
            response = ReadResponse(success=True, error=None, data=df_result)
        except Exception as e:
            response = ReadResponse(success=False, error=e, data=None)
        return response
