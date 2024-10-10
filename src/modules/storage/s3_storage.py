import boto3
import time
from botocore.exceptions import ClientError
from modules.storage.interface_storage import CoreStorageInterface
from modules.utils.logger import logger

class S3Storage(CoreStorageInterface):
    def __init__(self):
        
        self.s3 = boto3.client('s3', region_name="eu-west-1")
        self.bucket_name = ""
        logger.info(f'S3Storage initialized with bucket: {self.bucket_name}, region: XX')

    def read(self, path: str):
    
        logger.info(f'Reading from S3 path: {path}')
        return self._retry(lambda: self._read_object(path))

    def _read_object(self, path: str):
        
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=path)
            logger.info(f'Successfully read from path: {path}')
            return response['Body'].read()
        except ClientError as e:
            logger.error(f'Failed to read: {e}')
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

    def write(self, path: str, data):
        
        logger.info(f'Writing to S3 path: {path}')
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=path, Body=data)
            logger.info(f'Successfully wrote to path: {path}')
        except ClientError as e:
            logger.error(f'Failed to write: {e}')
            raise