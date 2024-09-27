from pyspark.sql import SparkSession
import logging
import boto3
import os
import sys
import argparse
import json
# from dataplatform_tools import SparkTool, GlueClientTool
from spark import SparkTool
from glue import GlueClientTool


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()
handler = logging.FileHandler('landing_to_raw.log', 'w', 'utf-8')
handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
logger.addHandler(handler)

logger = logging.getLogger()

aws_region = 'eu-west-1'

class ProcessingCoordinator:
    def __init__(self):
        logger.info('Initializing Spark application...')

        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue', aws_region)

        args_parser = argparse.ArgumentParser(description="Arguments")
        # --table,morningstar_classes,
        # --source_file,morningstar_classes/processed/classes_20240826_prueba_sf.csv,
        # --source_bucket,aihd1airas3aihgdp-landing,
        # --target_bucket,aihd1airas3aihgdp-raw
        args_parser.add_argument("--table", type=str, required=True, help="Table name")
        args_parser.add_argument("--source_file", type=str, required=True, help="File to load")
        args_parser.add_argument("--source_bucket", type=str, required=True, help="Source bucket")
        args_parser.add_argument("--target_bucket", type=str, required=True, help="Target bucket")
        self.args = args_parser.parse_args()

        self.config = self._read_json_config(self.s3_client,
                                             "-".join([self.args.source_bucket.split('-')[0], 'code']),
                                             "/".join([self.args.table,
                                                       'emr',
                                                       ".".join([os.path.basename(__file__).split(".")[0], 'json'])]))

        self._set_vars()

        self.spark = SparkSession.builder \
            .appName("Landing to Raw") \
            .getOrCreate()

        self.spark_tool = SparkTool(self.spark)
        self.glue_tool = GlueClientTool(self.glue_client)
    
    def process(self):
        logger.info("process")
        df_original = self._read_data()
        df_final = self._transformations(df_original)
        self._write_data(df_final)
        # self._create_partition()
        self.glue_tool.create_partition(logger,
                                         self.db_target_rl,
                                         self.args.table,
                                         self.config['target']['partition_field'],
                                         self.filedate)

    def _read_json_config(self, s3, bucket, key):
        logger.info("_read_json_config")
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body']
        json_object = json.loads(content.read())
        return json_object

    def _set_vars(self):
        logger.info("_set_vars")
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
        print(f'self.s3_source_path : {self.s3_source_path}')

        self.s3_target_path = "/".join(["s3:/",
                                        self.args.target_bucket,
                                        self.db_target,
                                        self.args.table])  # /{p_partition_field}}=20240917/"
        return

    def _read_data(self):
        logger.info("_read_data")
        if self.config['source']['filetype'] == "text":
            config_txt = self.config['source']['filetype_text']
            df = self.spark_tool.read_file_txt(config_txt, self.s3_source_path)

            df.show(10)
        return df

    def _write_data(self, df):
        logger.info("_write_data")
        partition = f"{self.config['target']['partition_field']}={self.filedate}"
        file = "/".join([self.s3_target_path, partition])

        logger.info(f"_write_data to {file}")
        df.show(10)

        target_filetype = self.config['target']['filetype']
        if target_filetype == "text":
            self.spark_tool.write_file_txt(df,
                                           self.config['target']['filetype_text'],
                                           self.config['target']['mode'],
                                           file)
        elif target_filetype == "parquet":
            self.spark_tool.write_file_parquet(df,
                                               self.config['target']['mode'],
                                               file)
        return

    def _transformations(self, df):
        logger.info("_transformations")
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


if __name__ == '__main__':
    try:
        stb = ProcessingCoordinator()
        stb.process()
    except Exception as error:
        msg_error = 'Exception processing data Landing to Raw: %s' % str(error)
        logger.error(msg_error)
        raise error
