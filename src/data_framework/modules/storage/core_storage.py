from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import Environment
from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse,
    ListResponse,
    PathResponse
)


class Storage:

    @LazyClassProperty
    def _storage(cls) -> CoreStorageInterface:
        if config().environment != Environment.LOCAL:
            from data_framework.modules.storage.integrations.s3_storage import S3Storage
            return S3Storage()
        else:
            from data_framework.modules.storage.integrations.local_storage import LocalStorage
            return LocalStorage()

    @classmethod
    def read(cls, layer: Layer, key_path: str, bucket: str = None) -> ReadResponse:
        return cls._storage.read(layer=layer, key_path=key_path, bucket=bucket)

    @classmethod
    def write(
        cls,
        layer: Layer,
        database: Database,
        table: str,
        data: bytes,
        partitions: str,
        filename: str
    ) -> WriteResponse:
        return cls._storage.write(
            layer=layer,
            database=database,
            table=table,
            data=data,
            partitions=partitions,
            filename=filename
        )

    @classmethod
    def write_to_path(cls, layer: Layer, key_path: str, data: bytes) -> WriteResponse:
        return cls._storage.write_to_path(
            layer=layer,
            key_path=key_path,
            data=data
        )

    @classmethod
    def list_files(cls, layer: Layer, prefix: str) -> ListResponse:
        return cls._storage.list_files(layer=layer, prefix=prefix)

    @classmethod
    def raw_layer_path(cls, database: Database, table_name: str) -> PathResponse:
        response = cls._storage.raw_layer_path(
            database=database,
            table_name=table_name
        )

        logger.info(f'response path ~> {response.path}')

        return response
    
    @classmethod
    def base_layer_path(cls, layer: Layer) -> PathResponse:
        return cls._storage.base_layer_path(layer=layer)