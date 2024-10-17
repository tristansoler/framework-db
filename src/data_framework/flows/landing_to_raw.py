from spark import SparkTool, SparkToolText, SparkToolParquet
from glue import GlueClientTool
from config import read_json_config_from_s3
from logger import configure_logger
from src.validation.file_validator import FileValidator
from src.dataplatform_tools.s3 import S3Client
# from file_validator import FileValidator
# from s3 import S3Client
import os
import argparse

from pyspark.sql import SparkSession


class ProcessingCoordinator:
    def __init__(self):
        # logger.info('Initializing ProcessingCoordinator...')
        self.logger = configure_logger('landing_to_raw', 'INFO')

        args_parser = argparse.ArgumentParser(description="Arguments")
        # --table,morningstar_classes,
        # --source_file,morningstar_classes/processed/classes_20240826_prueba_sf.csv,
        # --source_bucket,aihd1airas3aihgdp-landing,
        # --target_bucket,aihd1airas3aihgdp-raw
        # Namespace(table='producttest_tabla1', source_file='product_test/inbound/classes_20240922_prueba.csv',
        #           source_bucket='aihd1airas3aihgdp-landing', target_bucket='aihd1airas3aihgdp-raw')
        # Namespace(table='producttest_tabla1', source_file='product_test/inbound/classes_20240922_prueba.csv',
        #           source_bucket='aihd1airas3aihgdp-landing', target_bucket='aihd1airas3aihgdp-raw')
        args_parser.add_argument("--table", type=str, required=True, help="Table name")
        args_parser.add_argument("--source_file", type=str, required=True, help="File to load")
        args_parser.add_argument("--source_bucket", type=str, required=True, help="Source bucket")
        args_parser.add_argument("--target_bucket", type=str, required=True, help="Target bucket")
        self.args = args_parser.parse_args()
        print(self.args)

        s3_client = S3Client(self.logger)

        self.config = read_json_config_from_s3(s3_client,
                                               "-".join([self.args.source_bucket.split('-')[0], 'code']),
                                               "/".join([self.args.table,
                                                        'emr',
                                                         ".".join([os.path.basename(__file__).split(".")[0], 'json'])]))
        print(type(self.config))

        self._set_vars()

        self.app_name = "Landing to Raw"
        self.spark_tool_source = self._get_spark_tool_type(self.app_name, self.config['source']['filetype'])
        self.spark_tool_target = self._get_spark_tool_type(self.app_name, self.config['target']['filetype'])

        self._prueba_spark()

    def _prueba_spark(self):
        self.spark = SparkSession \
            .builder \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "funds_staging/") \
            .enableHiveSupport() \
            .getOrCreate()

        self.spark.sql("show databases").show()
        self.spark.sql("show catalogs").show()

        try:
            df = self.spark.sql("select * from rl_funds_raw.product_test limit 10")
            df.show(10)
        except Exception as error:
            print('ERRROR rl_funds_raw.product_test %s' % str(error))

        try:
            df = self.spark.sql("select * from rl_funds_raw.morningstar_classes limit 10")
            df.show(10)
        except Exception as error:
            print('ERRROR rl_funds_raw.morningstar_classes %s' % str(error))

        try:
            df = self.spark.sql("select * from rl_funds_staging.morningstar_dividends limit 10")
            df.show(10)
        except Exception as error:
            print('ERRROR rl_funds_staging.morningstar_dividends %s' % str(error))

        return

    def _set_vars(self):
        self.logger.info("_set_vars")
        self.db_target = self.config['target']['db']
        self.db_target_rl = self.config['target']['db_rl']
        self.filename = self.args.source_file.split("/")[-1]
        self.filedate = 'YYYYMMDD'

        if self.config['source']['filetype_text']['date_position'] == "filename":
            date_from = self.config['source']['filetype_text']['date_position_filename']['date_from']
            date_to = self.config['source']['filetype_text']['date_position_filename']['date_to']
            self.filedate = self.filename[date_from: date_to]

        self.s3_source_path = "/".join(["s3:/",
                                        self.args.source_bucket,
                                        self.args.table,
                                        "inbound"])
        self.s3_target_path = "/".join(["s3:/",
                                        self.args.target_bucket,
                                        self.db_target,
                                        self.args.table])  # /{p_partition_field}}=20240917/"
        return

    def _get_spark_tool_type(self, app_name: str, filetype: str) -> SparkTool:
        """
        Función para la creación de clase según parametro con tipo de fichero
        :param app_name: parámetro para la configuración de Spark
        :param filetype: tipo de fichero, para la creación del correspondiente SparkTool
        :return:
        """
        spark_tool_types = {
            'text': SparkToolText(app_name),
            'parquet': SparkToolParquet(app_name)
        }
        return spark_tool_types[filetype]

    def _read_data(self):
        self.logger.info("_read_data")

        if self.config['source']['filetype'] == "text":
            config_txt = self.config['source']['filetype_text']
            df = self.spark_tool_source.read_file(config_txt, self.s3_source_path)

            df.show(10)
        return df

    def _transformations(self, df):
        self.logger.info("_transformations")
        # mapping nombres de columnas..
        # Añadir columna de fecha del fichero
        # Añadir columna con nombre del fichero
        # Añadir columna con fecha inserción
        # Partición por fecha de inserción/fecha de fichero
        l_cols_landing = df.columns
        l_cols_raw = self.config["target"]["header_cols"].split("|")
        i = 0
        for col in l_cols_landing:
            df = df.withColumnRenamed(col, l_cols_raw[i])
            i = i + 1

        return df

    def _write_data(self, df):
        self.logger.info("_write_data")
        partition = f"{self.config['target']['partition_field']}={self.filedate}"
        file = "/".join([self.s3_target_path, partition])

        self. logger.info(f"_write_data to {file}")
        df.show(10)

        options = self.config['target']['filetype_text'] if self.config['target']['filetype'] == "text" else None
        self.spark_tool_target.write_file(df,
                                          self.config['target']['mode'],
                                          file,
                                          options)
        return

    def _validate_file(self):
        self.logger.info("_validate_file")
        file_validator = FileValidator(
            self.logger,
            'product_test/inbound/classes_20240925_prueba.csv',
            'product_test',
            'aihd1airas3aihgdp-landing',
            '',
            'aihd1airas3aihgdp-code',
            'product_test/emr/landing_to_raw.json'
        )
        file_validator.validate_file()

    def process(self):
        """
        Método principal. Consiste en:
        -lectura
        -transformación del dato
        -escritura
        -creación de la partición
        """
        self.logger.info("process")
        self._validate_file()
        df_original = self._read_data()
        df_final = self._transformations(df_original)
        self._write_data(df_final)
        glue_tool = GlueClientTool(self.logger)
        glue_tool.create_partition(self.db_target_rl,
                                   self.args.table,
                                   self.config['target']['partition_field'],
                                   self.filedate)


if __name__ == '__main__':
    try:
        stb = ProcessingCoordinator()
        stb.process()
    except Exception as error:
        msg_error = 'Exception processing data Landing to Raw: %s' % str(error)
        raise msg_error
