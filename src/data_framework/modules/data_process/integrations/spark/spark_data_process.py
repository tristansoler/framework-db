from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse,
)
from data_framework.modules.data_process.integrations.spark import utils as utils
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.helpers.cast import Cast
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.monitoring.core_monitoring import (
    CoreMonitoring,
    MetricNames
)
from data_framework.modules.config.model.flows import (
    DatabaseTable,
    ExecutionMode,
    CastingStrategy,
    LandingFileFormat,
    JSONFormat
)
from data_framework.modules.utils.debug import debug_code
from data_framework.modules.data_process.integrations.spark.dynamic_config import DynamicConfig
from data_framework.modules.exception.data_process_exceptions import (
    ReadDataError,
    WriteDataError,
    DataProcessError,
    CastDataError,
    DeleteDataError,
    SparkConfigurationError
)
from typing import List, Any, Union
from io import BytesIO
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when
from pyspark.sql.types import StructType
import pyspark.sql.functions as f
import time
import random
import inspect
import json

iceberg_exceptions = ['ConcurrentModificationExceptio', 'CommitFailedException', 'ValidationException']


class SparkDataProcess(DataProcessInterface):

    __iceberg_snapshot_metrics_map = {
        'added-records': MetricNames.TABLE_WRITE_ADDED_RECORDS,
        'added-files-size': MetricNames.TABLE_WRITE_ADDED_SIZE,
        'deleted-records': MetricNames.TABLE_WRITE_DELETED_RECORDS,
        'removed-files-size': MetricNames.TABLE_WRITE_DELETED_SIZE,
        'total-records': MetricNames.TABLE_WRITE_TOTAL_RECORDS,
        'total-files-size': MetricNames.TABLE_WRITE_TOTAL_SIZE
    }

    def __init__(self):
        try:
            # Obtain Spark configuration for the current process
            json_config = config().current_process_config().processing_specifications

            spark_config = SparkConf() \
                .setAppName(f"[{config().parameters.dataflow}] {config().parameters.process}")

            spark_config.setAll([
                # S3
                ("spark.sql.catalog.iceberg_catalog.http-client.apache.max-connections", "2000"),
                ("fs.s3.maxConnections", "100"),
                # Memory
                # ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
                # Iceberg
                ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
                ("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
                ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
                ("spark.jars", "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"),
                ("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
                ("spark.sql.catalog.iceberg_catalog.warehouse", "default_warehouse/"),
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


                ("spark.sql.sources.partitionOverwriteMode", 'DYNAMIC')
            ])

            extra_config = json_config.spark_configuration.config

            spark_config.setAll(pairs=extra_config.items())

            logger.info(spark_config.getAll())

            # Create Spark session
            self.spark = SparkSession.builder \
                .config(conf=spark_config) \
                .enableHiveSupport() \
                .getOrCreate()
        except Exception:
            raise SparkConfigurationError()
        # Others
        self.catalogue = CoreCatalogue()
        self.storage = Storage()
        self.__monitoring = CoreMonitoring()

    def _build_complete_table_name(self, database: str, table: str) -> str:
        return f'iceberg_catalog.{database}.{table}'

    def _track_table_metric(self, table_config: DatabaseTable, data_frame: DataFrame = None):

        if data_frame:
            self.__monitoring.track_table_metric(
                name=MetricNames.TABLE_READ_RECORDS,
                database=table_config.database.value,
                table=table_config.table,
                value=float(data_frame.count())
            )
        else:
            table_name = self._build_complete_table_name(
                database=table_config.database_relation,
                table=table_config.table
            )

            iceberg_table = self.spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
                self.spark._jsparkSession, table_name
            )

            snapshot = iceberg_table.currentSnapshot()

            if snapshot is not None:
                java_summary = snapshot.summary()

                iterator = java_summary.entrySet().iterator()
                while iterator.hasNext():
                    entry = iterator.next()
                    if entry.getKey() in self.__iceberg_snapshot_metrics_map.keys():
                        self.__monitoring.track_table_metric(
                            name=self.__iceberg_snapshot_metrics_map.get(entry.getKey()),
                            database=table_config.database.value,
                            table=table_config.table,
                            value=float(entry.getValue())
                        )

    def merge(self, dataframe: DataFrame, table_config: DatabaseTable, custom_strategy: str = None) -> WriteResponse:
        try:
            ""
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)

            source_method = inspect.stack()[2].function

            self.spark.sparkContext.setJobGroup(f"[MERGE] {source_method}", table_name, interruptOnCancel=True)

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
            logger.info(f'merge sql \n{merge_query}')
            self._execute_query(merge_query)

            self.spark.sparkContext.setJobGroup("", "", False)

            response = WriteResponse(success=True, error=None)
            self._track_table_metric(table_config=table_config)
            return response
        except Exception:
            raise WriteDataError(database=table_config.database_relation, table=table_config.table)

    def insert_overwrite(self, dataframe: Any, table_config: DatabaseTable) -> WriteResponse:
        try:
            ""
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)

            source_method = inspect.stack()[2].function

            self.spark.sparkContext.setJobGroup(f"[INSERT OVERWRITE] {source_method}", table_name, interruptOnCancel=True)

            # Select only necessary columns of the dataframe
            dataframe = self._select_table_columns(dataframe, table_config)

            dataframe.write.format("iceberg").mode('overwrite').save(table_name)

            self.spark.sparkContext.setJobGroup("", "", False)

            response = WriteResponse(success=True, error=None)
            self._track_table_metric(table_config=table_config)

            return response
        except Exception:
            raise WriteDataError(database=table_config.database_relation, table=table_config.table)

    def datacast(
        self,
        table_source: DatabaseTable,
        table_target: DatabaseTable
    ) -> ReadResponse:
        try:
            read_path = self.storage.raw_layer_path(
                database=table_source.database,
                table_name=table_source.table
            )

            logger.info(f"Casting strategy > {table_target.casting.strategy}")
            logger.info(f"Read path > {read_path.path}")

            if config().processes.landing_to_raw.incoming_file.file_format == LandingFileFormat.JSON:
                df_raw = self._read_raw_json_file(
                    data_path=read_path.relative_path,
                    casting_strategy=table_target.casting.strategy
                )
            elif table_target.casting.strategy == CastingStrategy.ONE_BY_ONE:
                # First, the raw data is read converting all the fields into strings
                schema_response = self.catalogue.get_schema(table_source.database_relation, table_source.table)
                columns = schema_response.schema.get_column_names(partitioned=True)
                spark_schema = utils.convert_schema_to_strings(columns=columns)
                df_raw = self._read_raw_file(
                    base_path=read_path.base_path,
                    data_path=read_path.path,
                    schema=spark_schema
                )
            elif table_target.casting.strategy == CastingStrategy.DYNAMIC:
                # The schema is inferred directly from the raw data
                df_raw = self._read_raw_file(base_path=read_path.base_path, data_path=read_path.path)

            self._track_table_metric(table_config=table_source, data_frame=df_raw)
            df_raw = utils.apply_transformations(df_raw, table_target.casting.transformations)

            if table_target.casting.strategy == CastingStrategy.ONE_BY_ONE:
                if table_target.casting.fix_incompatible_characters:
                    df_raw = utils.fix_incompatible_characters(
                        df_origin=df_raw,
                        table_target=table_target
                    )
                # After reading the data as strings, each field is converted to its corresponding data type
                view_name = 'data_to_cast'
                df_raw.createOrReplaceTempView(view_name)
                query = Cast().build_datacast_query(
                    source_columns=df_raw.columns,
                    table_target=table_target,
                    view_name=view_name
                )
                df_raw = self._execute_query(query)
            return ReadResponse(success=True, error=None, data=df_raw)
        except Exception:
            raise CastDataError(
                source_database=table_source.database_relation,
                source_table=table_source.table,
                target_database=table_target.database_relation,
                target_table=table_target.table,
                casting_strategy=table_target.casting.strategy.value
            )

    def _read_raw_file(self, base_path: str, data_path: str, schema: Union[StructType, None] = None) -> DataFrame:
        incoming_file = config().processes.landing_to_raw.incoming_file
        file_format = incoming_file.file_format
        spark_read_config = incoming_file.specifications.read_config
        final_data_path = None
        if config().parameters.execution_mode == ExecutionMode.DELTA:
            spark_read_config["basePath"] = base_path
            final_data_path = data_path
        else:
            final_data_path = base_path
        logger.info(f"read with spark options {spark_read_config}")
        spark_read = self.spark.read.options(**spark_read_config)
        if schema is not None:
            spark_read = spark_read.schema(schema)
        if file_format == LandingFileFormat.CSV:
            return spark_read.csv(final_data_path)
        else:
            return spark_read.parquet(final_data_path)

    def _read_raw_json_file(self, data_path: str, casting_strategy: CastingStrategy) -> DataFrame:
        # Read JSON file from S3
        file_path = data_path + config().parameters.file_name
        response = self.storage.read(layer=Layer.RAW, key_path=file_path)
        file_content = BytesIO(response.data)
        # Parse into a Python dictionary
        json_file = json.loads(file_content.getvalue())
        # Obtain JSON specifications
        json_specs = config().processes.landing_to_raw.incoming_file.json_specs
        # Obtain the data to be parsed into a DataFrame based on the specified path
        for key_level in json_specs.levels:
            json_file = json_file[key_level]
        if json_specs.source_level_format == JSONFormat.ARRAY:
            data = json_file
        elif json_specs.source_level_format == JSONFormat.DICTIONARY:
            data = list(json_file.values())
        # Transform into a DataFrame
        if casting_strategy == CastingStrategy.ONE_BY_ONE:
            # All fields are converted into strings
            columns = max(data, key=len).keys()
            schema = utils.convert_schema_to_strings(columns=columns)
            df = self.create_dataframe(data=data, schema=schema).data
            if debug_code:
                df.printSchema()
            return df
        elif casting_strategy == CastingStrategy.DYNAMIC:
            # Each field type is inferred by Spark
            df = self.create_dataframe(data=data).data
            if debug_code:
                df.printSchema()
            return df

    def _execute_query(self, query: str) -> DataFrame:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df_result = self.spark.sql(query)
                break
            except Exception as exception:
                if any(word in str(exception) for word in iceberg_exceptions) and attempt < max_retries - 1:
                    logger.warning(exception)
                    time.sleep(random.randint(1, 20))
                else:
                    raise exception
        return df_result

    def read_table(self, database: str, table: str, filter: str = None, columns: List[str] = None) -> ReadResponse:
        # TODO: use DatabaseTable instead of database and table strings
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
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise ReadDataError(query=query)

    def delete_from_table(self, table_config: DatabaseTable, _filter: str) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            query = f"DELETE FROM {table_name} WHERE {_filter}"
            self._execute_query(query)
            return WriteResponse(success=True, error=None)
        except Exception:
            raise DeleteDataError(database=table_config.database_relation, table=table_config.table)

    def insert_dataframe(self, dataframe: DataFrame, table_config: DatabaseTable) -> WriteResponse:
        try:
            table_name = self._build_complete_table_name(table_config.database_relation, table_config.table)
            # Select only necessary columns of the dataframe
            dataframe = self._select_table_columns(dataframe, table_config)
            # Insert dataframe into table
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    dataframe.writeTo(table_name).append()
                    return WriteResponse(success=True, error=None)
                except Exception as exception:
                    if any(word in str(exception) for word in iceberg_exceptions) and attempt < max_retries - 1:
                        logger.warning(exception)
                        time.sleep(random.randint(1, 20))
                    else:
                        raise exception
        except Exception:
            raise WriteDataError(database=table_config.database_relation, table=table_config.table)

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
                    df_2 = df_2.withColumnRenamed(right_column, left_column)
                df_result = df_1.join(df_2, on=left_on, how=how)
            return ReadResponse(success=True, error=None, data=df_result)
        except Exception:
            raise DataProcessError(error_message='Error performing join of two dataframes')

    def create_dataframe(self, data: Any, schema: str = None) -> ReadResponse:
        try:
            df = self.spark.createDataFrame(data, schema)
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise DataProcessError(error_message='Error creating dataframe')

    def query(self, sql: str) -> ReadResponse:
        try:
            df = self._execute_query(sql)
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise ReadDataError(query=sql)

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
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError()

    def unfold_string_values(self, dataframe: DataFrame, column_name: str, separator: str) -> ReadResponse:
        try:
            values = list(set(dataframe.filter(
                (f.col(column_name).isNotNull()) & (f.col(column_name) != '')
            ).select(
                f.explode(f.split(f.col(column_name), separator))
            ).rdd.flatMap(lambda x: x).collect()))
            return ReadResponse(success=True, error=None, data=values)
        except Exception:
            raise DataProcessError()

    def add_dynamic_column(
        self,
        dataframe: DataFrame,
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
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError()

    def stack_columns(
        self,
        dataframe: DataFrame,
        source_columns: List[str],
        target_columns: List[str]
    ) -> ReadResponse:
        try:
            if len(target_columns) != 2:
                raise ValueError(f'Must specify two columns as target_columns. Found {target_columns}')
            n_columns = len(source_columns)
            source_columns_str = ', '.join([f"'{column}', {column}" for column in source_columns])
            target_columns_str = ', '.join(target_columns)
            stack_expression = f"stack({n_columns}, {source_columns_str}) as ({target_columns_str})"
            dataframe = dataframe.select(*source_columns).selectExpr(stack_expression)
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError()

    def is_empty(self, dataframe: DataFrame) -> bool:
        if dataframe is not None:
            return dataframe.isEmpty()
        else:
            return True

    def count_rows(self, dataframe: DataFrame) -> int:
        return dataframe.count()

    def select_columns(self, dataframe: DataFrame, columns: List[str]) -> ReadResponse:
        try:
            dataframe = dataframe.select(*columns)
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError('Error selecting columns of a dataframe')

    def show_dataframe(self, dataframe: DataFrame) -> WriteResponse:
        dataframe.show(truncate=False)
