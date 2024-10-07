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

    def get_files_s3(self, bucket_name, key_name) -> list:
        """
        Función que recupera ficheros de un bucket y ruta.
        Devuelve una lista con ellos.
        TODO: NO PROBADO EN ESTE ENTORNO
        :param bucket_name: nombre del bucket donde buscar
        :param key_name: nombre de la ruta donde buscar
        :return: lista de ficheros en el bucket y ruta
        """
        try:
            response = self.s3_client.list_objects(Bucket=bucket_name, Prefix=key_name)
            l_files = []
            for obj in response.get('Contents', []):
                file_name = obj['Key'].split('/')[-1]
                l_files.append(file_name)
            return l_files
        except Exception as e:
            self.logger.error(f'Error obtaining files from bucket {bucket_name} and key {key_name}: {e}')
            return []

    def f_move_file(self, obj, to_bucket_name, to_key):
        """
        Función para mover objeto a un bucket destino y ruta destino.
        TODO: NO PROBADO EN ESTE ENTORNO
        :param obj: objeto a mover
        :param to_bucket_name: bucket de destino
        :param to_key: ruta de destino
        :return:
        """
        try:
            to_obj = obj.key
            self.s3_client.Object(to_bucket_name, to_key).copy_from(CopySource={'Bucket': obj.bucket_name, 'Key': obj.key})
            self.s3_client.Object(obj.bucket_name, obj.key).delete()
            print('* copy %s - delete file from source bucket %s' % (to_key, to_obj))
        except Exception as e:
            self.logger.error(f'Error moving file to {to_bucket_name} and key {to_key}: {e}')
        return

    def f_clean_s3(self, bucket_name, key):
        """
        Función para limpiar una carpeta de s3
        TODO: NO PROBADO EN ESTE ENTORNO
        :param to_bucket_name: bucket donde encontrar la carpeta a limpiar
        :param to_key: ruta a limpiar
        :return:
        """
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            bucket.objects.filter(Prefix=key).delete()
        except Exception as e:
            self.logger.error(f'Error cleaning {bucket_name} and key {key}: {e}')
        return
