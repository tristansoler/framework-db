import os
from io import BytesIO
import boto3
from src.dataplatform_tools.logger import configure_logger


class S3Client(object):

    def __init__(self) -> None:
        self.logger = configure_logger('S3 Client', 'INFO')
        self.profile_name = os.getenv('PROFILE_NAME', '')
        self.region = os.getenv('AWS_REGION_NAME', 'eu-west-1')
        self.session = self.configure_aws_session()
        self.s3_client = self.session.client('s3', region_name=self.region)
        self.s3_resource = self.session.resource('s3', region_name=self.region)

    def configure_aws_session(self) -> boto3.session.Session:
        """Configure the AWS session to be used depending on the
        profile name specified as an environment variable.
        """
        if self.profile_name:
            self.logger.info(f'Configuring AWS session using {self.profile_name} profile')
            return boto3.session.Session(region_name=self.region, profile_name=self.profile_name)
        else:
            self.logger.info('Configuring AWS session using the default profile')
            return boto3.session.Session(region_name=self.region)

    def get_file_content_from_s3(self, bucket_name: str, file_key: str) -> BytesIO:
        try:
            response = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=file_key
            )
            status_code = response['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                file_content = BytesIO(response['Body'].read())
                return file_content
            else:
                self.logger.info(
                    f'File {file_key} could not be obtained from bucket {bucket_name}. Status code: {status_code}'
                )
                return BytesIO()
        except Exception as e:
            self.logger.error(f'Error obtaining file {file_key} from bucket {bucket_name}: {e}')
            return BytesIO()
