import boto3
import time
from botocore.exceptions import ClientError
from data_framework.modules.storage.interface_storage import CoreStorageInterface
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Enviroment
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse
)

class S3Storage(CoreStorageInterface):
    def __init__(self):

        self.s3 = boto3.client('s3', region_name=config().parameters.region)

    def read(self, layer: Layer, database: Database, table: str) -> ReadResponse:
        bucket = self._build_bucket_name(layer=layer)
        key_path = self._build_s3_key_path(database=database, table=table)

        response: ReadResponse = None
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key_path)
            logger.info(f'Successfully read from path: {key_path}')
            s3_data = response['Body'].read()

            response = ReadResponse(
                data=s3_data,
                success=True
            )

            return 
        except ClientError as error:
            logger.error(f'Failed to read: {error}')

            response = ReadResponse(
                error=error,
                success=True
            )

        return response

    def _build_s3_bucket_name(self, layer: Layer) -> str:
        return f's3://{config().parameters.bucket_prefix}_{layer.value}'
    
    def _build_s3_key_path(self, database: Database, table: str, partitions: dict = {}) -> str:
        partitions_path = '/'.join([f"{partition_name}={value}" for partition_name, value in dict.items()])

        return f'{database.value}/{table}/{partitions_path}'

    def write(self, layer: Layer, database: Database, table: str, data: bytes, partitions: dict) -> WriteResponse:
        bucket = self._build_bucket_name(layer=layer)
        key_path = self._build_s3_key_path(database=database, table=table, partitions=partitions)

        logger.info(f'Writing to S3 path: {key_path}')
        
        response: ReadResponse = None

        try:
            self.s3.put_object(Bucket=bucket, Key=key_path, Body=data)
            logger.info(f'Successfully wrote to path: {key_path}')

            response = WriteResponse(success=True)
        except ClientError as error:
            logger.error(f'Failed to write: {error}')
            response = WriteResponse(success=False, error=error)
        
        return response