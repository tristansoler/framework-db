from abc import ABC, abstractmethod


class DataProcessInterface(ABC):

    @abstractmethod
    def update(self, flowdata: str, table_name: str):
        # Abstract class to define the basic storage interface
        pass

    @abstractmethod
    def delete(self, df: any, flowdata: str, table_name: str):
        # Abstract method to write data to a specific location
        pass

    @abstractmethod
    def merge(self):
        pass
