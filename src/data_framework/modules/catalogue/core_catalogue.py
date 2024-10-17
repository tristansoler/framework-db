from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.catalogue.interface_catalogue import CatalogueInterface
from typing import Union

class CoreCatalogue(object):

    @LazyClassProperty
    def _catalogue(cls) -> CatalogueInterface:
        from data_framework.modules.catalogue.integrations.aws_glue import CatalogueAWSGlue

        return CatalogueAWSGlue()

    @staticmethod
    def create_partition(database: str, table: str, partition_field: str, value: Union[str, int]):
        
        return CoreCatalogue._catalogue.create_partition(
            database,
            table,
            partition_field,
            value
        )

    @staticmethod
    def get_schema(database: str, table: str):
        
        return CoreCatalogue._catalogue.get_schema(
            database,
            table
        )
    
    