from modules.storage.core_storage import Storage
from typing import Type, TypeVar, get_type_hints
from modules.config.versions.v1 import Flows, LandingToRaw, IncomingFileLandingToRaw, DateLocatedFilename, Parameters, CSVSpecs, Config
import threading
import os
import sys

T = TypeVar('T')

def config() -> Config:
    
    return ConfigSetup()._instancia.config

class ConfigSetup:
    
    _instancia = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instancia is None:
            with cls._lock:
                if cls._instancia is None:
                    cls._instancia = super(ConfigSetup, cls).__new__(cls)
        return cls._instancia

    def __init__(self, parameters: dict = None):

        if not parameters:
            parameters = {}
            for i in range(1, len(sys.argv), 2):
                key = sys.argv[i].replace('--', '').replace('-', '_')
                value = sys.argv[i+1]
                parameters[key] = value

        # TODO: Pending
        os.getenv('ENV') == 'local'

        json_config = ConfigSetup.read_config_file(is_local=False)

        self._instancia.config = ConfigSetup.v1(model=Config, parameters=parameters, json_file=json_config)
    
    @classmethod
    def read_config_file(cls, is_local: bool) -> dict:
        # TODO: Pending
        import json
        from pathlib import Path

        path_absolute = Path(__file__).resolve()
        path_config = str(path_absolute.parent.parent.parent) + "\\tests\\resources\\configs\\v1.json"

        file = open(path_config)
        json = json.loads(file.read())
        json["environment"] = "local"

        return json


    @classmethod
    def v1(cls, model: Type[T], json_file: dict, parameters: dict = None) -> T:
        

        fieldtypes = get_type_hints(model)
        kwargs = {}

        for field, field_type in fieldtypes.items():
            if isinstance(field_type, type) and issubclass(field_type, (Flows, LandingToRaw, CSVSpecs, IncomingFileLandingToRaw, DateLocatedFilename)):
                kwargs[field] = cls.v1(model=field_type, json_file=json_file[field])
            elif isinstance(field_type, type) and issubclass(field_type, (Parameters)):
                kwargs[field] = cls.v1(model=field_type, json_file=parameters)
            else:
                kwargs[field] = json_file[field]
        return model(**kwargs)