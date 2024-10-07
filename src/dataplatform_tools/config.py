import boto3
import json


def read_json_config_from_s3(bucket, key):
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body']
        json_object = json.loads(content.read())
        return json_object
    except Exception as error:
        msg_error = f'Error in read_json_config_from_s3: {str(error)}'
        raise msg_error
