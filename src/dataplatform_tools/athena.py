import boto3
import time


class AthenaClientTool:

    def __init__(self):
        self.aws_region = 'eu-west-1'
        self.athena_client = boto3.client('athena', region_name=self.aws_region)

    def run_query(self, query, database_name, s3_output):
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']

        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return state, query_execution_id
            time.sleep(1)

    def get_query_execution(self, query_execution_id):
        response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        return response

 # # Ejecutar la consulta de inserción
 #    try:
 #        state, query_execution_id = run_query(insert_query, database, s3_output)
 #        if state == 'SUCCEEDED':
 #            print("Datos insertados correctamente en la tabla Iceberg.")
 #        else:
 #            print(f"Error al insertar datos. Estado: {state}")
 #            # Obtener detalles del error si es necesario
 #            error_details = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
 #            print(f"Detalles del error: {error_details}")
 #    except Exception as e:
 #        print(f"Error de AWS: {e}")