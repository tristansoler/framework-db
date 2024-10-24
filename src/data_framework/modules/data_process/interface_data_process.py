from abc import ABC, abstractmethod


class DataProcessInterface(ABC):

    @abstractmethod
    def merge(self):
        pass
