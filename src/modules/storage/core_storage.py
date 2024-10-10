from modules.utils.logger import Logger
from modules.code.lazy_class_property import LazyClassProperty
from modules.storage.interface_storage import CoreStorageInterface



class Storage:

    @LazyClassProperty
    def _storage(cls) -> CoreStorageInterface:
        from modules.storage.s3_storage import S3Storage
        from modules.storage.local_storage import LocalStorage

        # TODO: Implementar switch para obtener el storage necesario
        return S3Storage()
    
    @classmethod
    def read(cls, path: str):
        # Abstract class to define the basic storage interface
        pass

    @classmethod
    def write(cls, bucket: str ,path: str, data):
        print(Storage._storage.write(path = path, data = data))