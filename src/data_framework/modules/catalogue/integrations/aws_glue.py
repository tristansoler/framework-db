from data_framework.modules.catalogue.interface_catalogue import (
    CatalogueInterface,
    SchemaResponse,
    GenericResponse,
    Column,
    Schema
)
from data_framework.modules.exception.aws_exceptions import GlueError
from data_framework.modules.exception.catalogue_exceptions import (
    CreatePartitionError,
    InvalidPartitionFieldError,
    SchemaError
)
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from typing import Union, Any
import boto3


class CatalogueAWSGlue(CatalogueInterface):

    def __init__(self):
        self.cache = {}
        self.logger = logger
        self.config = config()
        self.glue_client = boto3.client('glue', region_name=config().parameters.region)

    def create_partition(
        self,
        database: str,
        table: str,
        partition_field: str,
        partition_value: Union[str, int]
    ) -> GenericResponse:
        """
        Function to create new partition in a table
        :param database: name of database of the table
        :param table: name of the table
        :param partition_field: name of the partition
        :param partition_value: value od the partition
        :return: GenericResponse
        """
        try:
            response = self.get_schema(database, table)
            l_cols_part = [
                column.name
                for column in response.schema.columns
                if column.ispartitioned is True
            ]
            if partition_field not in l_cols_part:
                raise InvalidPartitionFieldError(
                    database=database,
                    table=table,
                    partition_field=partition_field
                )
            else:
                try:
                    l_partitions_table = self.glue_client.get_partitions(
                        DatabaseName=database,
                        TableName=table
                    )
                except Exception as e:
                    raise GlueError(error_message=f'Error obtaining partitions of {database}.{table}: {e}')
                l_partitions_table_values = [elem['Values'] for elem in l_partitions_table['Partitions']]
                if [partition_value] in l_partitions_table_values:
                    self.logger.info(
                        f'Partition {partition_field}={partition_value} already exists in {database}.{table}'
                    )
                else:
                    table_gl = self._get_glue_table(database, table)
                    stg_desc_table_gl = table_gl['Table']['StorageDescriptor']
                    stg_desc = stg_desc_table_gl.copy()
                    location = stg_desc['Location']
                    location += f"/{partition_field}={partition_value}"
                    stg_desc['Location'] = location
                    partition_desc_l = [{
                        'Values': [partition_value],
                        'StorageDescriptor': stg_desc,
                        'Parameters': {}
                    }]
                    response_gc = self.glue_client.batch_create_partition(
                        DatabaseName=database,
                        TableName=table,
                        PartitionInputList=partition_desc_l
                    )
                    if 'Errors' in response_gc and response_gc['Errors']:
                        raise GlueError(error_message=str(response_gc['Errors']))
                    else:
                        self.logger.info(
                            f'Partition {partition_field}={partition_value} successfully created in {database}.{table}'
                        )
        except Exception:
            raise CreatePartitionError(
                database=database,
                table=table,
                partition_field=partition_field,
                partition_value=partition_value
            )
        else:
            return GenericResponse(success=True, error=None)

    def get_schema(self, database: str, table: str) -> SchemaResponse:
        """
        Function to get schema(list of columns(name and type) of a table
        :param database: name of database of the table
        :param table: name of the table
        :return: SchemaResponse
        """
        try:
            cache_key = f'schema.{database}.{table}'
            if cache_key in self.cache:
                return self.cache.get(cache_key)
            table_gl = self._get_glue_table(database, table)
            l_columns = table_gl['Table']['StorageDescriptor']['Columns']
            l_names = [column['Name'] for column in l_columns]
            l_types = [column['Type'] for column in l_columns]
            l_ispartitioned = [False for _ in l_columns]
            if table_gl['Table'].get('PartitionKeys'):
                l_partition_keys = table_gl['Table']['PartitionKeys']
                l_partition_keys_names = [column['Name'] for column in l_partition_keys]
                l_partition_keys_types = [column['Type'] for column in l_partition_keys]
                l_partition_keys_ispartitioned = [True for _ in l_partition_keys]
                l_names.extend(l_partition_keys_names)
                l_types.extend(l_partition_keys_types)
                l_ispartitioned.extend(l_partition_keys_ispartitioned)
            n_cols = len(l_names)
            l_order = [number+1 for number in range(n_cols)]
            l_schema_zip = list(zip(l_names, l_types, l_order, l_ispartitioned))
            l_schema = [Column(elem[0], elem[1], elem[2], elem[3]) for elem in l_schema_zip]
            schema = Schema(columns=l_schema)
            response = SchemaResponse(success=True, error=None, schema=schema)
            self.cache[cache_key] = response
            return response
        except Exception:
            raise SchemaError(database=database, table=table)

    def _get_glue_table(self, database: str, table: str) -> Any:
        try:
            response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            return response
        except Exception as e:
            raise GlueError(error_message=f'Error obtaining table {database}.{table}: {e}')
