from data_framework.modules.config.model.flows import Transformation
import sys
from typing import List, Dict, Any
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def apply_transformations(
    df: DataFrame,
    transformations: List[Transformation],
    **kwargs: Dict[str, Any]
) -> DataFrame:
    for transformation in transformations:
        try:
            transformation_function = getattr(sys.modules[__name__], transformation.type.value)
            df = transformation_function(df, transformation, **kwargs)
        except AttributeError:
            raise NotImplementedError(f'Transformation {transformation_function} not implemented')
    return df


def parse_dates(df: DataFrame, transformation: Transformation) -> DataFrame:
    for column in transformation.columns:
        if column not in df.columns:
            raise ValueError(
                f'Column {column} not found in raw DataFrame. Unable to apply transformation'
            )
        df = df.withColumn(
            column, f.date_format(
                f.to_date(f.col(column), transformation.source_format),
                transformation.target_format
            )
        )
    return df
