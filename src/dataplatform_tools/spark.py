


class SparkTool:

    def __init__(self, a_spark):
        self.spark = a_spark

    def read_file_txt(self, config_txt, s3_source_path):
        df = self.spark.read \
                .option('skipRows', config_txt['skip_rows']) \
                .option('sep', config_txt['delimiter']) \
                .option('header', config_txt['header']) \
                .option('inferSchema', config_txt['infer_schema']) \
                .option('encoding', config_txt['encoding']) \
                .csv(s3_source_path)
        return df

    def write_file_txt(self, df, options, mode, file_path):
        df.write.options(header=options['header'], delimiter=options['delimiter'], encoding=options['encoding']) \
                .mode(mode) \
                .csv(file_path)
        return mode

    def write_file_parquet(self, df,  mode, file_path):
        df.write \
            .mode(mode) \
            .parquet(file_path)
        return