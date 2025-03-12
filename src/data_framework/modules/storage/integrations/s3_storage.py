import boto3
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse,
    ListResponse,
    PathResponse
)
from data_framework.modules.exception.aws_exceptions import S3Error
from data_framework.modules.exception.storage_exceptions import (
    StorageError,
    StorageReadError,
    StorageWriteError
)

from data_framework.modules.config.model.flows import (
    ExecutionMode
)
from typing import NamedTuple


class BuildPathResponse(NamedTuple):
    base_path: str
    data_path: str = None


class S3Storage(CoreStorageInterface):

    def __init__(self):
        self.s3 = boto3.client('s3', region_name=config().parameters.region)

    def read(self, layer: Layer, key_path: str, bucket: str = None) -> ReadResponse:
        try:
            if bucket is None:
                bucket = self._build_s3_bucket_name(layer=layer)
            try:
                response = self.s3.get_object(Bucket=bucket, Key=key_path)
            except Exception:
                raise S3Error(error_message='Error obtaining file from S3')
            logger.info(f'Successfully read from path: {key_path}')
            s3_data = response['Body'].read()
            return ReadResponse(data=s3_data, success=True, error=None)
        except Exception:
            raise StorageReadError(path=f's3://{bucket}/{key_path}')

    def _build_s3_bucket_name(self, layer: Layer) -> str:
        return f'{config().parameters.bucket_prefix}-{layer.value}'

    def _build_s3_key_path(
        self,
        database: Database,
        table: str,
        partitions: dict = {},
        filename: str = ''
    ) -> BuildPathResponse:
        if partitions:
            partitions_path = '/'.join([f"{partition_name}={value}" for partition_name, value in partitions.items()])
            return BuildPathResponse(
                data_path=f'{database.value}/{table}/{partitions_path}/{filename}',
                base_path=f'{database.value}/{table}/'
            )
        else:
            return BuildPathResponse(
                data_path=f'{database.value}/{table}/{filename}',
                base_path=f'{database.value}/{table}/'
            )

    def write(
        self,
        layer: Layer,
        database: Database,
        table: str,
        data: bytes,
        filename: str,
        partitions: dict = None
    ) -> WriteResponse:
        try:
            bucket = self._build_s3_bucket_name(layer=layer)
            key_path = self._build_s3_key_path(
                database=database,
                table=table,
                partitions=partitions,
                filename=filename
            )
            try:
                self.s3.put_object(Bucket=bucket, Key=key_path.data_path, Body=data)
            except Exception:
                raise S3Error(error_message='Error uploading file to S3')
            logger.info(f'Successfully wrote to path: {key_path.data_path}')
            return WriteResponse(success=True, error=None)
        except Exception:
            raise StorageWriteError(path=f's3://{bucket}/{key_path.data_path}')

    def write_to_path(self, layer: Layer, key_path: str, data: bytes) -> WriteResponse:
        try:
            bucket = self._build_s3_bucket_name(layer=layer)
            try:
                self.s3.put_object(Bucket=bucket, Key=key_path, Body=data)
            except Exception:
                raise S3Error(error_message='Error uploading file to S3')
            logger.info(f'Successfully wrote to path: {key_path}')
            return WriteResponse(success=True, error=None)
        except Exception:
            raise StorageWriteError(path=f's3://{bucket}/{key_path}')

    def list_files(self, layer: Layer, prefix: str) -> ListResponse:
        try:
            bucket = self._build_s3_bucket_name(layer=layer)
            logger.info(f'Listing files from bucket {bucket} with prefix {prefix}')
            paginator = self.s3.get_paginator('list_objects_v2')
            keys = []
            for pagination_response in paginator.paginate(Bucket=bucket, Prefix=prefix):
                status_code = pagination_response['ResponseMetadata']['HTTPStatusCode']
                if status_code == 200 and 'Contents' in pagination_response:
                    file_keys = [obj['Key'] for obj in pagination_response.get('Contents', [])]
                    keys.extend(file_keys)
            return ListResponse(error=None, success=True, result=keys)
        except Exception:
            raise StorageError(
                error_message=f'Error listing files from {bucket} with prefix {prefix}'
            )

    def raw_layer_path(self, database: Database, table_name: str) -> PathResponse:
        s3_bucket = self._build_s3_bucket_name(layer=Layer.RAW)
        partitions = {}
        if config().parameters.execution_mode == ExecutionMode.DELTA:
            partitions[config().processes.landing_to_raw.output_file.partition_field] = config().parameters.file_date

        s3_key = self._build_s3_key_path(database=database, table=table_name, partitions=partitions)

        response = PathResponse(
            success=True,
            error=None,
            bucket=s3_bucket,
            path=f's3://{s3_bucket}/{s3_key.data_path}',
            base_path=f's3://{s3_bucket}/{s3_key.base_path}',
            relative_path=s3_key.data_path,
            relative_base_path=s3_key.base_path
        )
        return response

    def base_layer_path(self, layer: Layer) -> PathResponse:
        s3_bucket = self._build_s3_bucket_name(layer=layer)
        final_path = f's3://{s3_bucket}'
        response = PathResponse(
            success=True,
            error=None,
            bucket=s3_bucket,
            path=None,
            base_path=final_path,
            relative_path=None,
            relative_base_path=None
        )
        return response
