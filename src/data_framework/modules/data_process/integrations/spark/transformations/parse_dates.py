from data_framework.modules.config.model.flows import Transformation
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def parse_dates(df: DataFrame, transformation: Transformation) -> DataFrame:
    for column in transformation.columns:
        if column not in df.columns:
            raise ValueError(
                f'Column {column} not found in raw DataFrame. Unable to apply transformation'
            )
        for source_format in transformation.source_format:
            df = df.withColumn(
                column,
                f.when(
                    f.to_date(f.col(column), source_format).isNotNull(),
                    f.date_format(
                        f.to_date(f.col(column), source_format),
                        transformation.target_format
                    )
                ).otherwise(f.col(column))
            )
    return df
