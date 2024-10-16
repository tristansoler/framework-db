from modules.storage.core_storage import Storage
from typing import Type, TypeVar, get_type_hints
from config.model.flows import (
    Flows,
    LandingToRaw,
    IncomingFileLandingToRaw,
    DateLocatedFilename,
    Parameters,
    CSVSpecs,
    Config,
    OutputFile,
    Partitions,
    Validations,
    ProcessingSpecifications,
    Hardware,
    Enviroment
)
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

        json_config = ConfigSetup.read_config_file(dataflo=parameters.get('dataflow'), is_local=False)

        self._instancia.config = ConfigSetup.v1(model=Config, parameters=parameters, json_file=json_config)

    @classmethod
    def read_config_file(cls, dataflow: str, is_local: bool) -> dict:
        # TODO: Pending
        import json
        from pathlib import Path

        path_absolute = Path(__file__).resolve()
        path_config = str(path_absolute.parent.parent.parent) + "\\tests\\resources\\configs\\ms.json"

        file = open(path_config)
        config_json = dict(json.loads(file.read()))

        common_flow_json = current_flow_json = config_json.get('common')
        current_flow_json = config_json.get(dataflow, None)

        if current_flow_json is None:
            current_flow_json = common_flow_json
        else:
            current_flow_json = cls.merged_current_dataflow_with_common(
                current_dataflow=current_flow_json,
                common=common_flow_json
            )

        final_json = {
            "environment": "local",
            "flows": current_flow_json
        }

        return final_json
    
    @classmethod
    def merged_current_dataflow_with_common(cls, current_dataflow: dict, common: dict) -> dict:
        
        merged = current_dataflow.copy()

        for key, value in common.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = cls.merged_current_dataflow_with_common(merged[key], value)
        else:
            merged[key] = value

        return merged

    @classmethod
    def v1(cls, model: Type[T], json_file: dict, parameters: dict = None) -> T:

        fieldtypes = get_type_hints(model)
        kwargs = {}

        models = (
            Flows, LandingToRaw, CSVSpecs, IncomingFileLandingToRaw,
            DateLocatedFilename, OutputFile, Partitions, Validations, ProcessingSpecifications,
            Hardware
        )

        for field, field_type in fieldtypes.items():
            if isinstance(field_type, type) and issubclass(field_type, models):
                kwargs[field] = cls.v1(model=field_type, json_file=json_file[field])
            elif isinstance(field_type, type) and issubclass(field_type, (Parameters)):
                kwargs[field] = cls.v1(model=field_type, json_file=parameters)
            else:
                kwargs[field] = json_file[field]
        return model(**kwargs)
