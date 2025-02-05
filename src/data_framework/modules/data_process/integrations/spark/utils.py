from data_framework.modules.catalogue import interface_catalogue as catalogue
from data_framework.modules.config.model.flows import Transformation
from importlib import import_module
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import (
     IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType
)

def convert_schema(schema: catalogue.Schema) -> StructType:
    schema_columns = []

    for column in schema.columns:
        struct_field = StructField(column.name, StringType(), True)
        schema_columns.append(struct_field)

    return StructType(schema_columns)




def map_to_spark_type(db_type: str):

    mapping = {
        "string": StringType(),
        "varchar": StringType(),
        "char": StringType(),
        "text": StringType(),
        "integer": IntegerType(),
        "int": IntegerType(),
        "bigint": IntegerType(),
        "smallint": IntegerType(),
        "tinyint": IntegerType(),
        "decimal": DoubleType(),
        "numeric": DoubleType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }

    return mapping.get(db_type.lower(), StringType())


def apply_transformations(
    df: DataFrame,
    transformations: List[Transformation],
    **kwargs: Dict[str, Any]
) -> DataFrame:
    for transformation in transformations:
        try:
            function_name = transformation.type.value
            module_name = f'data_framework.modules.data_process.integrations.spark.transformations.{function_name}'
            module = import_module(module_name)
            transformation_function = getattr(module, function_name)
            df = transformation_function(df, transformation, **kwargs)
        except (ModuleNotFoundError, AttributeError):
            raise NotImplementedError(f'Transformation {function_name} not implemented')
    return df
