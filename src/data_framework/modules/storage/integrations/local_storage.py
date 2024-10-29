from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.storage.interface_storage import (
    CoreStorageInterface,
    Database,
    Layer,
    ReadResponse,
    WriteResponse,
    ListResponse
)
import os


class LocalStorage(CoreStorageInterface):
    def __init__(self):
        self.logger = logger

    def read(self, layer: Layer, key_path: str) -> ReadResponse:
        try:
            folder = self._build_folder_name(layer)
            full_path = os.path.join(folder, os.path.normpath(key_path))
            if os.path.exists(full_path):
                with open(full_path, 'rb') as f:
                    data = f.read()
                response = ReadResponse(error=None, success=True, data=data)
            else:
                message = f'Path {full_path} does not exist'
                self.logger.info(message)
                response = ReadResponse(error=message, success=False, data=None)
        except Exception as error:
            self.logger.error(f'Failed to read: {error}')
            response = ReadResponse(error=error, success=False, data=None)
        return response

    def _build_folder_name(self, layer: Layer) -> str:
        return f'{config().parameters.bucket_prefix}-{layer.value}'

    def _build_file_path(
        self,
        database: Database,
        table: str,
        partitions: dict = {},
        filename: str = ''
    ) -> str:
        if partitions:
            partitions_path = '/'.join([f"{partition_name}={value}" for partition_name, value in partitions.items()])
            return f'{database.value}/{table}/{partitions_path}/{filename}'
        else:
            return f'{database.value}/{table}/{filename}'

    def write(
        self,
        layer: Layer,
        database: Database,
        table: str,
        data: bytes,
        filename: str,
        partitions: dict = None
    ) -> WriteResponse:
        try:
            folder = self._build_folder_name(layer=layer)
            key_path = self._build_file_path(
                database=database,
                table=table,
                partitions=partitions,
                filename=filename
            )
            full_path = os.path.join(folder, os.path.normpath(key_path))
            parent_path = os.path.dirname(full_path)
            os.makedirs(parent_path, exist_ok=True)
            with open(full_path, 'wb') as f:
                f.write(data)
            logger.info(f'Successfully wrote to path: {full_path}')
            response = WriteResponse(success=True, error=None)
        except Exception as error:
            logger.error(f'Failed to write: {error}')
            response = WriteResponse(success=False, error=error)
        return response

    def list_files(self, layer: Layer, prefix: str) -> ListResponse:
        try:
            folder = self._build_folder_name(layer=layer)
            full_path = os.path.join(folder, os.path.normpath(prefix))
            if os.path.exists(full_path):
                logger.info(f'Listing files from path {full_path}')
                files_list = [
                    entry.path for entry in os.scandir(full_path) if entry.is_file()
                ]
                return ListResponse(error=None, success=True, result=files_list)
            else:
                message = f'Path {full_path} does not exist'
                self.logger.info(message)
                return ListResponse(error=message, success=False, result=[])
        except Exception as error:
            logger.error(f'Error listing files: {error}')
            return ListResponse(error=error, success=False, result=[])
