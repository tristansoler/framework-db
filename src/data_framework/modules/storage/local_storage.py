from data_framework.modules.storage.interface_storage import CoreStorageInterface
from data_framework.modules.utils.logger import Logger


class LocalStorage(CoreStorageInterface):
    def __init__(self):
        self.logger = Logger()._instance.logger
        self.logger.info(f'LocalStorage initialized')

    def read_from_path(self, path: str):
        # TODO: update local storage methods
        pass

    def read(self, path: str):

        self.logger.info(f'Reading: {path}')
        return ""

    def write(self, path: str, data):
        self.logger.info(f'Writing: {path}')
        try:
            self.logger.info("PENDING")
        except Exception as error:
            self.logger.error(f'Failed to write: {error}')
            raise
