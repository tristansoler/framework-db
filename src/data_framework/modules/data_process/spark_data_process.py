from data_framework.modules.data_process.interface_data_process import DataProcessInterface
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.data_process.helpers.cast import Cast
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

        # self.spark = SparkSession \
        #     .builder \
        #     .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        #     .config("spark.sql.catalog.iceberg_catalog.warehouse", 'funds_staging/') \
        #     .enableHiveSupport() \
        #     .getOrCreate()

    def merge(self, df: DataFrame, table_name: str):
        # df.createOrReplaceTempView("data_to_merge")
        # # TODO: Recuperar de la configuración los PKs para poder crear el SQL
        # primary_keys = []
        # sql_update_with_pks = '\n'.join([
        #     f'AND data_to_merge.{field} = {table_name}.{field}' for field in primary_keys
        # ])
        # merge_query = f"""
        #     MERGE INTO {table_name}
        #     USING data_to_merge ON
        #         {sql_update_with_pks}
        #     WHEN MATCHED THEN
        #     UPDATE SET *
        #     WHEN NOT MATCHED THEN
        #     INSERT *
        # """
        # self.spark.sql(merge_query)
        pass

    def datacast(
        self,
        database_source: str,
        table_source: str,
        where_source: str,
        database_target: str,
        table_target: str
    ) -> DataFrame:
        cast = Cast()
        query = cast.get_query_datacast(
            database_source,
            table_source,
            where_source,
            database_target,
            table_target
        )
        df_result = self.spark.sql(query)
        return df_result
