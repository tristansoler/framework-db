from data_framework.modules.config.model.flows import (
    Transformation,
    DatabaseTable,
    JSONFormat
)
from data_framework.modules.config.core import config
from data_framework.modules.exception.data_process_exceptions import (
    TransformationNotImplementedError,
    TransformationError
)
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from importlib import import_module
from typing import List, Dict, Any
from io import BytesIO
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType
)
import re
import json


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

    def repl(match):
        key = match.group(1)
        new_key = re.sub(r'\W', '', key).lower()
        return f'"{new_key}"'

    # key=value => "key": "value"
    json = re.sub(r'([^{},=\s]+)=([^,}]+)', r'"\1": "\2"', json)
    # '@key' => "key" or "@key" => "key"
    json = re.sub(r"[\"']([^\"']+)[\"'](?=\s*:)", repl, json)
    # 'key': => "key":
    json = re.sub(r"'([^']+)'(\s*:\s*)", r'"\1"\2', json)
    # :'value' => :"value"
    json = re.sub(r"(:\s*)'([^']+)'", r'\1"\2"', json)

    return json


def fix_incompatible_characters(df_origin: DataFrame, table_target: DatabaseTable) -> DataFrame:
    catalogue = CoreCatalogue()
    schema_target = catalogue.get_schema(table_target.database_relation, table_target.table).schema
    target_columns = schema_target.get_column_type_mapping(partitioned=True)

    df_modified = df_origin
    udf_fix_column_incompatible_characters = f.udf(fix_column_incompatible_characters, StringType())

    for field in df_origin.schema.fields:
        new_field_name = re.sub(r'\W', '', field.name)
        target_type = target_columns.get(field.name.lower(), 'string')
        if 'struct<' in target_type:
            df_modified = df_modified.withColumn(
                field.name,
                udf_fix_column_incompatible_characters(f.col(field.name))
            )
        if field.name != new_field_name:
            df_modified = df_modified.withColumnRenamed(field.name, new_field_name)

    return df_modified


def parse_json(files: List[BytesIO]) -> List[dict]:
    # Obtain JSON specifications
    json_specs = config().processes.landing_to_raw.incoming_file.json_specs
    partition_field = config().processes.landing_to_raw.output_file.partition_field
    data = []
    for file in files:
        for line in file.readlines():
            json_data = json.loads(line)
            data_date = json_data.get(partition_field, config().parameters.file_date)
            # Obtain the data to be parsed into a DataFrame based on the specified json path
            for key_level in json_specs.levels:
                try:
                    json_data = json_data[key_level]
                except KeyError:
                    raise KeyError(f'Path {json_specs.source_level} not found in JSON file')
            if json_specs.source_level_format == JSONFormat.DICTIONARY:
                json_data = list(json_data.values())
            [item.update({partition_field: data_date}) for item in json_data]
            data += json_data
    return data
