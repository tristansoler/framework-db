from abc import ABC, abstractmethod

class CoreStorageInterface(ABC):
    @abstractmethod
    def read(self, path: str):
        # Abstract class to define the basic storage interface
        pass

    @abstractmethod
    def write(self, path: str, data):
        # Abstract method to write data to a specific location
        pass