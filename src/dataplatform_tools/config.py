import json
from src.dataplatform_tools.s3 import S3Client


def read_json_config_from_s3(s3_client: S3Client, bucket: str, key: str) -> dict:
    try:
        content = s3_client.get_file_content_from_s3(bucket, key)
        json_object = json.load(content)
        return json_object
    except Exception as error:
        msg_error = f'Error in read_json_config_from_s3: {str(error)}'
        # TODO: raise specific exception
        raise msg_error
