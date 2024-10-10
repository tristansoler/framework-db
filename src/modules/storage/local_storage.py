from modules.storage.interface_storage import CoreStorageInterface
from modules.utils.logger import Logger

class LocalStorage(CoreStorageInterface):
    def __init__(self):
        self.logger = Logger()._instance.logger
        self.logger.info(f'LocalStorage initialized')

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