from spark import SparkTool, SparkToolText, SparkToolParquet
from glue import GlueClientTool
from config import read_json_config_from_s3
import logging
import os
import sys
import argparse




logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()
handler = logging.FileHandler('landing_to_raw.log', 'w', 'utf-8')
handler.setFormatter(logging.Formatter('%(name)s %(message)s'))
logger.addHandler(handler)

logger = logging.getLogger()


class ProcessingCoordinator:
    def __init__(self):
        logger.info('Initializing ProcessingCoordinator...')

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


        self.config = read_json_config_from_s3("-".join([self.args.source_bucket.split('-')[0], 'code']),
                                             "/".join([self.args.table,
                                                       'emr',
                                                       ".".join([os.path.basename(__file__).split(".")[0], 'json'])]))
        print(type(self.config))

        self._set_vars()

        self.app_name = "Landing to Raw"
        self.spark_tool_source = self._get_spark_tool_type(self.app_name, self.config['source']['filetype'])
        self.spark_tool_target = self._get_spark_tool_type(self.app_name, self.config['target']['filetype'])


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
        logger.info("_read_data")

        if self.config['source']['filetype'] == "text":
            config_txt = self.config['source']['filetype_text']
            df = self.spark_tool_source.read_file(config_txt, self.s3_source_path)

            df.show(10)
        return df

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

    def _write_data(self, df):
        logger.info("_write_data")
        partition = f"{self.config['target']['partition_field']}={self.filedate}"
        file = "/".join([self.s3_target_path, partition])

        logger.info(f"_write_data to {file}")
        df.show(10)

        options = self.config['target']['filetype_text'] if self.config['target']['filetype'] == "text" else None
        self.spark_tool_target.write_file(df,
                                          self.config['target']['mode'],
                                          file,
                                          options)
        return

    def process(self):
        """
        Método principal. Consiste en:
        -lectura
        -transformación del dato
        -escritura
        -creación de la partición
        """
        logger.info("process")
        df_original = self._read_data()
        df_final = self._transformations(df_original)
        self._write_data(df_final)
        glue_tool = GlueClientTool(logger)
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
        logger.error(msg_error)
        raise error
