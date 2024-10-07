import boto3


class GlueClientTool:

    def __init__(self):
        self.aws_region = 'eu-west-1'
        self.glue_client = boto3.client('glue', self.aws_region)

    def create_partition(self, logger, db, table, partition_field, filedate):
        try:
            logger.info(f'GlueClientTool.create_partition')

            l_partitions_table = self.get_partitions(logger, db, table)
            logger.info(f'Partitions of {db}.{table} : {str(l_partitions_table)}')

            partition_filedate = [filedate]
            if partition_filedate in l_partitions_table:
                logger.info(f'GlueClientTool.create_partition - Partition exists yet')
                return

            table_gl = self.glue_client.get_table(DatabaseName=db, Name=table)

            stg_desc_table_gl = table_gl['Table']['StorageDescriptor']
            stg_desc = stg_desc_table_gl.copy()
            location = stg_desc['Location']
            location += f"/{partition_field}={filedate}"
            stg_desc['Location'] = location

            partition_desc = {
                'Values': [filedate],
                'StorageDescriptor': stg_desc,
                'Parameters': {}
            }
            partition_desc_l = [partition_desc]
            logger.info(f'particiones:{str(partition_desc_l)}')

            response = self.glue_client.batch_create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInputList=partition_desc_l
            )

            if 'Errors' in response and response['Errors']:
                logger.info(f"Error creating partition {location}: {response['Errors']}")
            else:
                logger.info(f'New partition created: datadate:{location}')

            return

        except Exception as error:
            msg_error = f'Error in create_partition: {str(error)}'
            raise msg_error

    def get_partitions(self, logger, db, table):
        try:
            logger.info(f'GlueClientTool.get_partitions')
            l_partitions = []
            response = self.glue_client.get_partitions(
                DatabaseName=db,
                TableName=table
            )

            i = 0
            while i < len(response['Partitions']):
                l_partitions.append(response['Partitions'][i]['Values'])
                i = i + 1

            return l_partitions

        except Exception as error:
            msg_error = f'Error in get_partitions: {str(error)}'
            raise msg_error

    def _obtain_glue_columns(self, db, table):

        response = self.glue_client.get_table(DatabaseName=db, Name=table)

        schema = response['Table']['StorageDescriptor']['Columns']
        columns = [column['Name'] for column in schema]

        return columns
