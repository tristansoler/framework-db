from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import Platform
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.storage.core_storage import Storage
from data_framework.modules.exception.aws_exceptions import AthenaError
import boto3
from pandas import read_csv, DataFrame
from time import sleep
from io import BytesIO
from typing import Union


class AthenaClient:

    def __init__(self):
        self.logger = logger
        self.athena_client = boto3.client('athena', region_name=config().parameters.region)
        self.storage = Storage()
        # TODO: remove when migrating Infinity to Data Platform
        self.layer = (
            Layer.TEMP if config().platform == Platform.INFINITY
            else Layer.ATHENA
        )
        self.output_path = f's3://{config().parameters.bucket_prefix}-{self.layer.value}/{config().project_id}'

    def execute_query(self, query: str, read_output: bool = True) -> Union[DataFrame, None]:
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': self.output_path}
            )
            query_execution_id = response['QueryExecutionId']
            output_location = self.wait_for_query_to_complete(query_execution_id)
            if read_output:
                df = self.get_query_results(output_location)
                return df
        except Exception:
            raise AthenaError(error_message=f'Error executing the following query: {query}')

    def wait_for_query_to_complete(self, query_execution_id: str) -> str:
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status == 'SUCCEEDED':
                output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                return output_location
            elif status == 'FAILED':
                error = response['QueryExecution']['Status']['AthenaError']['ErrorMessage']
                raise AthenaError(error_message=f'Query execution has failed. Error: {error}')
            elif status == 'CANCELLED':
                raise AthenaError(error_message='Query execution has been cancelled')
            else:
                sleep(2)

    def get_query_results(self, output_location: str) -> DataFrame:
        result_path = output_location.replace('s3://', '').split('/', 1)[1]
        response = self.storage.read(self.layer, result_path)
        return read_csv(BytesIO(response.data))
