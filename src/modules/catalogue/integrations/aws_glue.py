from modules.catalogue.interface_catalogue import CatalogueInterface, SchemaResponse, GenericResponse
from modules.utils.logger import logger
from modules.config.core import config
from typing import Union
import boto3

class CatalogueAWSGlue(CatalogueInterface):

    def __init__(self):

        self.glue_client = boto3.client('glue', config().parameters.region)

    def create_partition(self, database: str, table: str, partition_field: str, partition_value: Union[str, int]) -> GenericResponse:

        try:
            logger.info(f'GlueClientTool.create_partition {db}.{table} - {str(partition_field)} - {filedate}')

            l_partitions_table = self.get_partitions(db, table)
            logger.info(f'Partitions of {db}.{table} : {str(l_partitions_table)}')

            partition_filedate = [filedate]
            if partition_filedate in l_partitions_table:
                logger.info(f'GlueClientTool.create_partition - Partition already exists.')
                return

            table_gl = self.glue_client.get_table(DatabaseName=db, Name=table)

            stg_desc_table_gl = table_gl['Table']['StorageDescriptor']
            stg_desc = stg_desc_table_gl.copy()
            location = stg_desc['Location']
            location += f"/{partition_field}={filedate}"
            stg_desc['Location'] = location

            partition_desc = {
                'Values': [filedate],
                'StorageDescriptor': stg_desc,
                'Parameters': {}
            }
            partition_desc_l = [partition_desc]
            logger.info(f'Partitions:{str(partition_desc_l)}')

            response = self.glue_client.batch_create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInputList=partition_desc_l
            )

            if 'Errors' in response and response['Errors']:
                logger.info(f"Error creating partition {location}: {response['Errors']}")
            else:
                logger.info(f'New partition created: datadate:{location}')

            return

        except Exception as error:
            msg_error = f'Error in create_partition: {str(error)}'
            raise msg_error
    
    def get_schema(self, database: str, table: str) -> SchemaResponse:
        try:
            logger.info(f'GlueClientTool.get_table_columns {database}.{table}')

            response = self.glue_client.get_table(DatabaseName=database, Name=table)

            schema = response['Table']['StorageDescriptor']['Columns']
            columns = [column['Name'] for column in schema]

            return columns

        except Exception as error:
            msg_error = f'Error in get_table_columns: {str(error)}'
            raise msg_error