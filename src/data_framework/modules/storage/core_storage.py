from data_framework.modules.utils.logger import Logger
from data_framework.modules.config.core import config

from data_framework.modules.config.model.flows import Enviroment

from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse
)

class Storage:

    @LazyClassProperty
    def _storage(cls) -> CoreStorageInterface:
        if config().environment == Enviroment.REMOTE:
            from data_framework.modules.storage.s3_storage import S3Storage
            return S3Storage()
        else:
            from data_framework.modules.storage.local_storage import LocalStorage
            return LocalStorage()
    
    @classmethod
    def read(cls, layer: Layer, database: Database, table: str) -> ReadResponse:
        return cls._storage.read(layer =layer, database = database, table = table)

    @classmethod
    def write(cls, layer: Layer, database: Database, table: str, data: bytes, partitions: str) -> WriteResponse:
        return cls._storage.write(layer =layer, database = database, table = table, data = data, partitions = partitions)