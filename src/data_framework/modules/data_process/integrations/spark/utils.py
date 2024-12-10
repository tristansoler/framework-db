from pyspark.sql.types import StructType, StructField, StringType
from data_framework.modules.catalogue import interface_catalogue as catalogue


def convert_schema(schema: catalogue.Schema) -> StructType:
    schema_columns = []

    for column in schema.columns:
        struct_field = StructField(column.name, StringType(), True)
        schema_columns.append(struct_field)

    return StructType(schema_columns)