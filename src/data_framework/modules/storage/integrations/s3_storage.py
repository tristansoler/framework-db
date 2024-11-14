import boto3
from botocore.exceptions import ClientError
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse,
    ListResponse
)


class S3Storage(CoreStorageInterface):
    def __init__(self):

        self.s3 = boto3.client('s3', region_name=config().parameters.region)

    def read(self, layer: Layer, key_path: str) -> ReadResponse:
        bucket = self._build_s3_bucket_name(layer=layer)
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key_path)
            logger.info(f'Successfully read from path: {key_path}')
            s3_data = response['Body'].read()
            response = ReadResponse(data=s3_data, success=True, error=None)
        except ClientError as error:
            logger.error(f'Failed to read: {error}')
            response = ReadResponse(error=error, success=False, data=None)
        return response

    def _build_s3_bucket_name(self, layer: Layer) -> str:
        return f'{config().parameters.bucket_prefix}-{layer.value}'

    def _build_s3_key_path(
        self,
        database: Database,
        table: str,
        partitions: dict = {},
        filename: str = ''
    ) -> str:
        if partitions:
            partitions_path = '/'.join([f"{partition_name}={value}" for partition_name, value in partitions.items()])
            return f'{database.value}/{table}/{partitions_path}/{filename}'
        else:
            return f'{database.value}/{table}/{filename}'

    def write(
        self,
        layer: Layer,
        database: Database,
        table: str,
        data: bytes,
        filename: str,
        partitions: dict = None
    ) -> WriteResponse:
        bucket = self._build_s3_bucket_name(layer=layer)
        key_path = self._build_s3_key_path(
            database=database,
            table=table,
            partitions=partitions,
            filename=filename
        )
        logger.info(f'Writing to S3 path: {key_path}')
        try:
            self.s3.put_object(Bucket=bucket, Key=key_path, Body=data)
            logger.info(f'Successfully wrote to path: {key_path}')
            response = WriteResponse(success=True, error=None)
        except ClientError as error:
            logger.error(f'Failed to write: {error}')
            response = WriteResponse(success=False, error=error)
        return response

    def write_to_path(self, layer: Layer, key_path: str, data: bytes) -> WriteResponse:
        bucket = self._build_s3_bucket_name(layer=layer)
        logger.info(f'Writing to S3 path: {key_path}')
        try:
            self.s3.put_object(Bucket=bucket, Key=key_path, Body=data)
            logger.info(f'Successfully wrote to path: {key_path}')
            response = WriteResponse(success=True, error=None)
        except ClientError as error:
            logger.error(f'Failed to write: {error}')
            response = WriteResponse(success=False, error=error)
        return response

    def list_files(self, layer: Layer, prefix: str) -> ListResponse:
        bucket = self._build_s3_bucket_name(layer=layer)
        logger.info(f'Listing files from bucket {bucket} with prefix {prefix}')
        try:
            list_response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            status_code = list_response['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                file_keys = [obj['Key'] for obj in list_response.get('Contents', [])]
                return ListResponse(error=None, success=True, result=file_keys)
            else:
                return ListResponse(error=f'Status code: {status_code}', success=False, result=[])
        except ClientError as error:
            logger.error(f'Error listing files: {error}')
            return ListResponse(error=error, success=False, result=[])
