import boto3


class GlueClientTool:

    def __init__(self):
        self.aws_region = 'eu-west-1'
        self.glue_client = boto3.client('glue', self.aws_region)

    def create_partition(self, logger, db_target, table, partition_field, filedate):
        logger.info(f'GlueClientTool._create_partition')

        table_gl = self.glue_client.get_table(DatabaseName=db_target, Name=table)

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
            DatabaseName=db_target,
            TableName=table,
            PartitionInputList=partition_desc_l
        )

        if 'Errors' in response and response['Errors']:
            logger.info(f"Erros creating partition {location}: {response['Errors']}")
        else:
            logger.info(f'New partition created: datadate:{location}')

        return
