from data_framework.modules.config.model.flows import Transformation, DatabaseTable
from data_framework.modules.exception.data_process_exceptions import (
    TransformationNotImplementedError,
    TransformationError
)
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from importlib import import_module
from typing import List, Dict, Any
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType
)
import re


def convert_schema_to_strings(columns: List[str]) -> StructType:
    return StructType([
        StructField(column, StringType(), True)
        for column in columns
    ])


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
            raise TransformationNotImplementedError(transformation=function_name)
        except Exception:
            raise TransformationError(transformation=function_name)
    return df


def fix_column_incompatible_characters(json):
    find_keys = r"'([^\"]+)'(?=\s*:)"
    fix_name_regex = r'[^a-zA-Z0-9_]'

    def repl(match):
        key = match.group(1)

        new_key = re.sub(fix_name_regex, '', key).lower()
        return f'"{new_key}"'

    fixed_key_names = re.sub(find_keys, repl, json)
    # 'key' => : "key"
    fixed_key_names = re.sub(r"'([^']+)'(\s*:\s*)", r'"\1"\2', fixed_key_names)
    # 'value' => : "value"
    fixed_key_names = re.sub(r"(:\s*)'([^']+)'", r'\1"\2"', fixed_key_names)

    return fixed_key_names


def fix_incompatible_characters(df_origin: DataFrame, table_target: DatabaseTable) -> DataFrame:
    fix_name_regex = r'[^a-zA-Z0-9_]'

    catalogue = CoreCatalogue()
    schema_target = catalogue.get_schema(table_target.database_relation, table_target.table).schema

    df_modified = df_origin
    udf_fix_column_incompatible_characters = f.udf(fix_column_incompatible_characters, StringType())

    for index, field in enumerate(df_origin.schema.fields):
        new_field_name = re.sub(fix_name_regex, '', field.name)
        target_column = schema_target.columns[index]

        if target_column.type in 'struct<':
            df_modified = df_modified.withColumn(
                field.name,
                udf_fix_column_incompatible_characters(f.col(field.name))
            )

        df_modified = df_modified.withColumnRenamed(field.name, new_field_name)

    return df_modified
