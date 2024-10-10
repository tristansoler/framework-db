import boto3
import time
from botocore.exceptions import ClientError
from modules.storage.core_storage import CoreStorageInterface
from data_framework.src.modules.utils.logger import Logger


class S3Storage(CoreStorageInterface):
    def __init__(self, bucket_name, region_name):
        
        self.s3 = boto3.client('s3', region_name=region_name)
        self.bucket_name = bucket_name
        self.logger = Logger()
        self.logger.info(f'S3Storage initialized with bucket: {bucket_name}, region: {region_name}')

    def read(self, path: str):
    
        self.logger.info(f'Reading from S3 path: {path}')
        return self._retry(lambda: self._read_object(path))

    def _read_object(self, path: str):
        
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=path)
            self.logger.info(f'Successfully read from path: {path}')
            return response['Body'].read()
        except ClientError as e:
            self.logger.error(f'Failed to read: {e}')
            raise

    def _retry(self, func, retries=3, backoff=2):
        
        for attempt in range(retries):
            try:
                return func()
            except ClientError as e:
                self.logger.error(f'Attempt {attempt + 1} failed: {e}')
                if attempt < retries - 1:
                    time.sleep(backoff ** attempt)
                    self.logger.info(f'Retrying attempt {attempt + 2} after backoff')
                else:
                    self.logger.error(f'All retry attempts failed operation')
                    raise

    def write(self, path: str, data):
        
        self.logger.info(f'Writing to S3 path: {path}')
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)
            self.logger.info(f'Successfully wrote to path: {path}')
        except ClientError as e:
            self.logger.error(f'Failed to write: {e}')
            raise