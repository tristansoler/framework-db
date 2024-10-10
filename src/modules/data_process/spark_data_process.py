class SparkDataProcess(DataProcessInterface):
    def __init__(self):
        #TODO: Tener en cuenta la config para saber si hay que incluir párametros a la hora de configurar spark

        self.spark = XXXX

    def merge(self, dataFrame: any, table_name: str):

        dataFrame.createOrReplaceTempView("data_to_merge")

        # TODO: Recuperar de la configuración los PKs para poder crear el SQL
        # sql_update_with_pks ='\n'.join([f'AND data_to_merge.{field} = {table_name}.{field}' for field in primary_keys])
        
        merge_query = f"""
            MERGE INTO {table_name}
            USING data_to_merge ON
                {sql_update_with_pks}
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT *
        """

        self.spark.sql(merge_query)