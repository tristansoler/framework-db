from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Environment
from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse,
    ListResponse
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
    def read(cls, layer: Layer, key_path: str) -> ReadResponse:
        return cls._storage.read(layer=layer, key_path=key_path)

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
    def list_files(cls, layer: Layer, prefix: str) -> ListResponse:
        return cls._storage.list_files(layer=layer, prefix=prefix)
