from data_framework.modules.config.model.flows import Transformation, TransformationType
from typing import List
from pyspark.sql import DataFrame


def apply_transformations(df: DataFrame, transformations: List[Transformation]) -> DataFrame:
    for transformation in transformations:
        pass
    return df