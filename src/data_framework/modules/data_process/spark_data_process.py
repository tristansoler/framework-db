from data_framework.modules.data_process.interface_data_process import DataProcessInterface
from data_framework.modules.data_process.helpers.cast import Cast
from pyspark.sql import DataFrame

class SparkDataProcess(DataProcessInterface):
    def __init__(self):
        #TODO: Tener en cuenta la config para saber si hay que incluir párametros a la hora de configurar spark

        self.spark = "TODO"

    def update(self, flowdata: str, table_name: str):
        # Abstract class to define the basic storage interface
        pass

    def delete(self, dataFrame: any, flowdata: str, table_name: str):
        # Abstract method to write data to a specific location
        pass

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
    
    def datacast(self,
            database_source: str,
            table_source: str,
            where_source:str,
            database_target: str,
            table_target: str) -> DataFrame:

        cast = Cast()
        query = cast.get_query_datacast(database_source, table_source, where_source, database_target, table_target)
        df_result = self.spark.sql(query) 

        return df_result