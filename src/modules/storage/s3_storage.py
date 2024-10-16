import boto3
import time
from botocore.exceptions import ClientError
from modules.storage.interface_storage import CoreStorageInterface
from modules.utils.logger import logger
from modules.config.core import config
from modules.config.model.flows import Enviroment
from modules.storage.interface_storage import (
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

        return self._retry(lambda: self._read_object(path))

    def _read_object(self, bucket: str, key_path: str):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key_path)
            logger.info(f'Successfully read from path: {key_path}')
            return response['Body'].read()
        except ClientError as e:
            logger.error(f'Failed to read: {e}')
            response = ReadResponse()
            raise

    def _retry(self, func, retries=3, backoff=2):
        
        for attempt in range(retries):
            try:
                return func()
            except ClientError as e:
                logger.error(f'Attempt {attempt + 1} failed: {e}')
                if attempt < retries - 1:
                    time.sleep(backoff ** attempt)
                    logger.info(f'Retrying attempt {attempt + 2} after backoff')
                else:
                    logger.error(f'All retry attempts failed operation')
                    raise

    def _build_s3_bucket_name(self, layer: Layer) -> str:
        return f's3://{config().parameters.bucket_prefix}_{layer.value}'
    
    def _build_s3_key_path(self, database: Database, table: str) -> str:
        return f'{database.value}/{table}/'


    def write(self, path: str, data):
        
        logger.info(f'Writing to S3 path: {path}')
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)
            logger.info(f'Successfully wrote to path: {path}')
        except ClientError as e:
            logger.error(f'Failed to write: {e}')
            raise