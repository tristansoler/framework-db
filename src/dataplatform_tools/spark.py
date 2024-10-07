from pyspark.sql import SparkSession
from abc import ABC, abstractmethod

class SparkTool(ABC):

    def __init__(self, appname):
        self.spark = SparkSession.builder.appName(appname).getOrCreate()

    def read_file(self, config_txt, s3_source_path):
        pass

    def write_file(self, df,  mode, file_path, options=None):
        pass


class SparkToolText(SparkTool):
    def read_file(self, config_txt, s3_source_path):
        df = self.spark.read \
                .option('skipRows', config_txt['skip_rows']) \
                .option('sep', config_txt['delimiter']) \
                .option('header', config_txt['header']) \
                .option('inferSchema', config_txt['infer_schema']) \
                .option('encoding', config_txt['encoding']) \
                .csv(s3_source_path)
        return df

    def write_file(self, df, mode, file_path, options):
        df.write.options(header=options['header'], delimiter=options['delimiter'], encoding=options['encoding']) \
                .mode(mode) \
                .csv(file_path)
        return

class SparkToolParquet(SparkTool):
    def read_file(self, config_txt, s3_source_path):
        return None

    def write_file(self, df,  mode, file_path, options=None):
        df.write \
            .mode(mode) \
            .parquet(file_path)
        return

class SparkToolBD(SparkTool):
    def read_file(self, config_txt, s3_source_path):
        pass

    def write_file(self, df,  mode, file_path, options=None):
        pass

    def execute_query(self, query):
        try:
            df = self.spark.sql(query)
            df.show()
        except Exception as error:
            msg_error = f'Error in execute_query: {str(error)}'
            raise msg_error
        return df

    def retrieve(self, database, table, columns, filter_where) -> object:
        try:
            query_args = {
                "columns": columns,
                "database": database,
                "table": table,
                "filter": filter_where
            }
            query = """
                select {columns}
                from {database}.{table}
                where {filter}""".format(**query_args)
            df = self.execute_query(query)


        except Exception as error:
            msg_error = f'Error in retrieve: {str(error)}'
            raise msg_error

        return df