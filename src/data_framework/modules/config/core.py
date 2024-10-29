from typing import Type, TypeVar, get_type_hints
from data_framework.modules.config.model.flows import (
    Processes,
    LandingToRaw,
    RawToStaging,
    ToOutput,
    IncomingFileLandingToRaw,
    DateLocatedFilename,
    Parameters,
    CSVSpecs,
    Config,
    DatabaseTable,
    Validations,
    ProcessingSpecifications,
    Hardware,
    Environment,
    SparkConfiguration,
    CustomConfiguration,
    OutputReport
)
import threading
import sys

T = TypeVar('T')


def config() -> Config:

    return ConfigSetup()._instancia.config


class ConfigSetup:

    _instancia = None
    _lock = threading.Lock()

    _models = (
        Processes, LandingToRaw, RawToStaging, ToOutput, CSVSpecs, IncomingFileLandingToRaw,
        DateLocatedFilename, DatabaseTable, Validations, ProcessingSpecifications,
        Hardware, SparkConfiguration, CustomConfiguration, OutputReport
    )

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

        is_local = parameters.get('environment') == Environment.LOCAL.value
        dataflow = parameters.get('dataflow')
        json_config = ConfigSetup.read_config_file(dataflow=dataflow, is_local=is_local)

        self._instancia.config = ConfigSetup.parse_to_model(model=Config, parameters=parameters, json_file=json_config)

    @classmethod
    def read_config_file(cls, dataflow: str, is_local: bool) -> dict:
        import json
        from pathlib import Path

        config_json: dict = None
        path_absolute = Path(__file__).resolve()
        environment = Environment.REMOTE
        if is_local:
            path_config = str(path_absolute.parent.parent.parent) + f'\\tests\\resources\\configs\\{dataflow}.json'
            file = open(path_config)
            config_json = dict(json.loads(file.read()))
            environment = Environment.LOCAL
        else:
            import zipfile

            transformation_path = str(path_absolute.parent.parent.parent.parent.parent) + '/transformation.zip'
            archive = zipfile.ZipFile(transformation_path, 'r')
            config_file = archive.open('config.json')
            config_json = dict(json.loads(config_file.read()))
            config_file.close()
        common_flow_json = config_json.get('common')
        current_flow_json = config_json.get(dataflow, None)
        if current_flow_json is None:
            current_flow_json = common_flow_json
        else:
            current_flow_json = cls.merged_current_dataflow_with_common(
                current_dataflow=current_flow_json,
                common=common_flow_json
            )
        current_flow_json['environment'] = environment
        return current_flow_json

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
    def parse_to_model(cls, model: Type[T], json_file: dict, parameters: dict = None) -> T:

        fieldtypes = get_type_hints(model)
        kwargs = {}

        for field, field_type in fieldtypes.items():
            if isinstance(field_type, type) and issubclass(field_type, cls._models):
                kwargs[field] = cls.parse_to_model(model=field_type, json_file=json_file.get(field))
            elif isinstance(field_type, type) and issubclass(field_type, (Parameters)):
                kwargs[field] = cls.parse_to_model(model=field_type, json_file=parameters)
            else:
                kwargs[field] = json_file.get(field)
        return model(**kwargs)
