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
from typing import List, Any
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import when
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
        json_config = config().current_process_config().processing_specifications            

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
            ("spark.sql.catalog.iceberg_catalog.warehouse", f"{json_config.spark_configuration.warehouse}/"),
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
            ("spark.dynamicAllocation.enabled", 'true'),
            ("spark.dynamicAllocation.initialExecutors", '2'),
            ("spark.dynamicAllocation.maxExecutors", '10'),
            
            # TODO: Set dynamic values from config
            #("spark.executor.instances", f'{json_config.hardware.instances}'),
            ("spark.executor.memory", f'{json_config.hardware.ram}m'),
            ("spark.executor.cores", f'{json_config.hardware.cores}')
            #("spark.driver.cores", f'{json_config.hardware.driver_cores}')
            
        ])
        

        # Add custom configurations
        for custom_config in json_config.spark_configuration.custom_configuration:
            spark_config.set(custom_config.parameter, custom_config.value)
        # Create Spark session
        self.spark = SparkSession.builder \
            .config(conf=spark_config) \
            .enableHiveSupport() \
            .getOrCreate()
        # Others
        self.catalogue = CoreCatalogue()

    def _build_complete_table_name(self, database: str, table: str) -> str:
        return f'iceberg_catalog.{database}.{table}'

    def merge(self, dataframe: DataFrame, table_config: DatabaseTable, custom_strategy: str = None) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            view_name = 'data_to_merge'
            # Select only necessary columns of the dataframe
            dataframe = self._select_table_columns(dataframe, table_config)
            # Perform merge
            dataframe.createOrReplaceTempView(view_name)
            sql_update_with_pks = '\n AND '.join([
                f' {view_name}.{field} = {table_name}.{field}' for field in table_config.primary_keys
            ])

            stratgy = """
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """
            
            if custom_strategy:
                stratgy = custom_strategy

            merge_query = f"""
                MERGE INTO {table_name}
                USING {view_name} ON
                    {sql_update_with_pks}
                {stratgy}
            """
            logger.debug(f'merge sql \n{merge_query}')
            self._execute_query(merge_query)
            response = WriteResponse(success=True, error=None)
        except Exception as e:
            logger.error(e)
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
            logger.error(e)
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def _execute_query(self, query: str) -> DataFrame:
        df_result = self.spark.sql(query)
        return df_result

    def read_table(self, database: str, table: str, filter: str = None, columns: List[str] = None) -> ReadResponse:
        try:
            table_name = self._build_complete_table_name(database=database, table=table)
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
            error_message = f"{e}\nSQL\n{query}"
            logger.error(error_message)
            response = ReadResponse(success=False, error=error_message, data=None)
        return response

    def delete_from_table(self, table_config: DatabaseTable, _filter: str) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            query = f"DELETE FROM {table_name} WHERE {_filter}"
            self._execute_query(query)
            response = WriteResponse(success=True, error=None)
        except Exception as e:
            error_message = f"{e}\nSQL\n{query}"
            logger.error(error_message)
            response = WriteResponse(success=False, error=error_message)
        return response

    def insert_dataframe(self, dataframe: DataFrame, table_config: DatabaseTable) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            # Select only necessary columns of the dataframe
            dataframe = self._select_table_columns(dataframe, table_config)
            # Insert dataframe into table
            dataframe.writeTo(table_name).append()
            response = WriteResponse(success=True, error=None)
        except Exception as e:
            logger.error(e)
            response = WriteResponse(success=False, error=e)
        return response

    def _select_table_columns(self, dataframe: DataFrame, table_config: DatabaseTable) -> DataFrame:
        table_schema = self.catalogue.get_schema(
            database=table_config.database_relation,
            table=table_config.table
        )
        table_columns = table_schema.schema.get_column_names(partitioned=True)
        dataframe = dataframe.select(*table_columns).distinct()
        return dataframe

    def join(
        self,
        df_1: DataFrame,
        df_2: DataFrame,
        how: str,
        left_on: List[str],
        right_on: List[str] = None,
        left_suffix: str = '_df_1',
        right_suffix: str = '_df_2'
    ) -> ReadResponse:
        try:
            if how not in ['inner', 'left', 'right', 'outer']:
                raise ValueError(
                    f'Invalid parameter value: how={how}. Allowed values: inner, left, right, outer'
                )
            if not right_on:
                right_on = left_on
            # Make a copy of the dataframes
            df_1 = df_1.alias('df_1')
            df_2 = df_2.alias('df_2')
            # Rename common columns before the join
            common_columns = list(set(df_1.columns) & set(df_2.columns))
            for column in common_columns:
                if column not in left_on:
                    df_1 = df_1.withColumnRenamed(column, column + left_suffix)
                if column not in right_on:
                    df_2 = df_2.withColumnRenamed(column, column + right_suffix)
            # Perform join
            if left_on == right_on:
                df_result = df_1.join(df_2, on=left_on, how=how)
            elif len(left_on) != len(right_on):
                raise ValueError(
                    'Number of columns in left_on and right_on parameters must be the same. ' +
                    f'left_on: {len(left_on)} columns. right_on: {len(right_on)} columns'
                )
            else:
                for left_column, right_column in zip(left_on, right_on):
                    if left_column != right_column:
                        df_2 = df_2.withColumnRenamed(right_column, left_column)
                df_result = df_1.join(df_2, on=left_on, how=how)
            # TODO: revisar tipo de respuesta. ¿TransformationResponse?
            response = ReadResponse(success=True, error=None, data=df_result)
        except Exception as e:
            logger.error(e)
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def create_dataframe(self, data: Any, schema: dict = None) -> ReadResponse:
        try:
            if schema:
                spark_schema = self._parse_schema(schema)
                df = self.spark.createDataFrame(data, spark_schema)
            else:
                df = self.spark.createDataFrame(data)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            logger.error(e)
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

    def query(self, sql: str) -> ReadResponse:
        try:
            df = self._execute_query(sql)
            response = ReadResponse(success=True, error=None, data=df)
        except Exception as e:
            error_message = f"{e}\nSQL\n{sql}"
            logger.error(error_message)
            response = ReadResponse(success=False, error=error_message, data=None)
        return response

    def overwrite_columns(
        self,
        dataframe: DataFrame,
        columns: List[str],
        custom_column_suffix: str,
        default_column_suffix: str,
        drop_columns: bool = True
    ) -> ReadResponse:
        try:
            for column in columns:
                custom_column = column + custom_column_suffix
                default_column = column + default_column_suffix
                dataframe = dataframe.withColumn(
                    column,
                    f.when(
                        f.col(custom_column).isNull(), f.col(default_column)
                    ).otherwise(f.col(custom_column))
                )
                if drop_columns:
                    dataframe = dataframe.drop(f.col(custom_column), f.col(default_column))
            response = ReadResponse(success=True, error=None, data=dataframe)
        except Exception as e:
            logger.error(e)
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def unfold_string_values(self, dataframe: DataFrame, column_name: str, separator: str) -> ReadResponse:
        try:
            values = list(set(dataframe.filter(
                (f.col(column_name).isNotNull()) & (f.col(column_name) != '')
            ).select(
                f.explode(f.split(f.col(column_name), separator))
            ).rdd.flatMap(lambda x: x).collect()))
            response = ReadResponse(success=True, error=None, data=values)
        except Exception as e:
            logger.error(e)
            response = ReadResponse(success=False, error=e, data=None)
        return response

    def add_dynamic_column(
        self,
        dataframe: Any,
        new_column: str,
        reference_column: str,
        available_columns: List[str],
        default_value: Any = None
    ) -> ReadResponse:
        try:
            if available_columns:
                # Build conditional expression for the new column
                expression = None
                for column in available_columns:
                    if expression is None:
                        # First item
                        expression = when(f.col(reference_column) == column, f.col(column))
                    else:
                        expression = expression.when(f.col(reference_column) == column, f.col(column))
                expression.otherwise(default_value)
            else:
                expression = f.lit(None)
            dataframe = dataframe.withColumn(new_column, expression)
            response = ReadResponse(success=True, error=None, data=dataframe)
        except Exception as e:
            logger.error(e)
            response = ReadResponse(success=False, error=e, data=None)
        return response
