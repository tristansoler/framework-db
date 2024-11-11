from data_framework.modules.catalogue.interface_catalogue import (
    CatalogueInterface,
    SchemaResponse,
    GenericResponse,
    Column,
    Schema
)
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from typing import Union
import boto3


class CatalogueAWSGlue(CatalogueInterface):

    def __init__(self):

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
            l_cols_part = [column.name for column in response.schema.columns if column.ispartitioned is True]

            if partition_field not in l_cols_part:
                success = False
                msg_error = (f'GlueClientTool.create_partition - Partition Field'
                             f' {partition_field} is not a field in table {table}.')
                self.logger.info(msg_error)
                return GenericResponse(success=success, error=msg_error)

            else:
                l_partitions_table = self.glue_client.get_partitions(DatabaseName=database, TableName=table)
                l_partitions_table_values = [elem['Values'] for elem in l_partitions_table['Partitions']]

                if [partition_value] in l_partitions_table_values:
                    success = True
                    msg_error = 'GlueClientTool.create_partition - Partition already exists.'
                    self.logger.info(msg_error)
                    return GenericResponse(success=success, error=msg_error)

                else:
                    table_gl = self.glue_client.get_table(DatabaseName=database, Name=table)
                    stg_desc_table_gl = table_gl['Table']['StorageDescriptor']
                    stg_desc = stg_desc_table_gl.copy()
                    location = stg_desc['Location']
                    location += f"/{partition_field}={partition_value}"
                    stg_desc['Location'] = location

                    partition_desc = {
                        'Values': [partition_value],
                        'StorageDescriptor': stg_desc,
                        'Parameters': {}
                    }
                    partition_desc_l = [partition_desc]

                    response_gc = self.glue_client.batch_create_partition(
                        DatabaseName=database,
                        TableName=table,
                        PartitionInputList=partition_desc_l
                    )

                    if 'Errors' in response_gc and response_gc['Errors']:
                        success = False
                        msg_error = f"Error creating partition {location}: {response_gc['Errors']}"
                        self.logger.info(msg_error)
                    else:
                        success = True
                        msg_error = None
                        self.logger.info(f'New partition created: {partition_field}:{partition_value}')

                    response = GenericResponse(success=success, error=msg_error)
                    return response

        except Exception as error:
            msg_error = f'Error in create_partition: {str(error)}'
            response = GenericResponse(success=False, error=msg_error)
            return response

    def get_schema(self, database: str, table: str) -> SchemaResponse:
        """
        Function to get schema(list of columns(name and type) of a table
        :param database: name of database of the table
        :param table: name of the table
        :return: SchemaResponse
        """
        try:
            response_gc = self.glue_client.get_table(DatabaseName=database, Name=table)

            if 'Errors' in response_gc and response_gc['Errors']:
                msg_error = f"Error get_schema {database}.{table}: {response_gc['Errors']}"
                self.logger.info(msg_error)
                response = SchemaResponse(success=False, error=msg_error, schema=None)
                return response

            else:
                l_columns = response_gc['Table']['StorageDescriptor']['Columns']
                l_names = [column['Name'] for column in l_columns]
                l_types = [column['Type'] for column in l_columns]
                l_ispartitioned = [False for _ in l_columns]

                try:
                    l_partition_keys = response_gc['Table']['PartitionKeys']
                    l_partition_keys_names = [column['Name'] for column in l_partition_keys]
                    l_partition_keys_types = [column['Type'] for column in l_partition_keys]
                    l_partition_keys_ispartitioned = [True for _ in l_partition_keys]
                    l_names.extend(l_partition_keys_names)
                    l_types.extend(l_partition_keys_types)
                    l_ispartitioned.extend(l_partition_keys_ispartitioned)
                except Exception:
                    self.logger.info(f"La tabla '{database}'.'{table}' no tiene particiones")

                n_cols = len(l_names)
                l_order = [number+1 for number in range(n_cols)]

                l_schema_zip = list(zip(l_names, l_types, l_order, l_ispartitioned))
                l_schema = [Column(elem[0], elem[1], elem[2], elem[3]) for elem in l_schema_zip]
                schema = Schema(columns=l_schema)
                response = SchemaResponse(success=True, error=None, schema=schema)
                return response

        except Exception as error:
            msg_error = f'Error in get_schema: {str(error)}'
            raise Exception(msg_error)
