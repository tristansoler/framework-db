from data_framework.modules.catalogue import interface_catalogue as catalogue
from data_framework.modules.config.model.flows import Transformation
import os
from importlib import import_module
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType


def convert_schema(schema: catalogue.Schema) -> StructType:
    schema_columns = []

    for column in schema.columns:
        struct_field = StructField(column.name, StringType(), True)
        schema_columns.append(struct_field)

    return StructType(schema_columns)


def apply_transformations(
    df: DataFrame,
    transformations: List[Transformation],
    **kwargs: Dict[str, Any]
) -> DataFrame:
    for transformation in transformations:
        module_names = list_transformation_modules(
            'data_framework.modules.data_process.integrations.spark.transformations'
        )
        transformation_function = get_transformation_function(
            module_names, transformation.type.value
        )
        df = transformation_function(df, transformation, **kwargs)
    return df


def list_transformation_modules(package: str) -> List[str]:
    package_module = import_module(package)
    modules = [
        f'{package}.{filename[:-3]}'
        for filename in os.listdir(package_module.__path__[0])
        if filename.endswith('.py') and filename != '__init__.py'
    ]
    return modules


def get_transformation_function(module_names: List[str], function_name: str) -> Any:
    for module_name in module_names:
        try:
            module = import_module(module_name)
            function = getattr(module, function_name)
            return function
        except (ModuleNotFoundError, AttributeError):
            pass
    raise NotImplementedError(f'Transformation {function_name} not implemented')
