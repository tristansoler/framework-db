from typing import Type, TypeVar, get_type_hints
from data_framework.modules.config.model.flows import (
    Processes,
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

    _models = (
        Processes, LandingToRaw, CSVSpecs, IncomingFileLandingToRaw,
        DateLocatedFilename, OutputFile, Partitions, Validations, ProcessingSpecifications,
        Hardware, LandingToRaw
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
        
        is_local = os.getenv('ENV') == 'local'
        dataflow = parameters.get('dataflow')
        bucket_prefix = parameters.get('bucket_prefix')
        flow = parameters.get('flow')

        json_config = ConfigSetup.read_config_file(dataflow=dataflow, bucket_prefix=bucket_prefix, flow=flow, is_local=is_local)

        self._instancia.config = ConfigSetup.parse_to_model(model=Config, parameters=parameters, json_file=json_config)

    @classmethod
    def read_config_file(cls, dataflow: str, bucket_prefix: str, flow: str, is_local: bool) -> dict:
        import json

        config_json: dict = None

        environment = "remote"

        if is_local:
            from pathlib import Path
            path_absolute = Path(__file__).resolve()
            path_config = str(path_absolute.parent.parent.parent) + f'\\tests\\resources\\configs\\{dataflow}.json'

            file = open(path_config)
            config_json = dict(json.loads(file.read()))
            
            environment = "local"
        else:
            import boto3

            s3 = boto3.client('s3')
            bucket = f'{bucket_prefix}_code'
            key_path = f'{dataflow}/config/transformations.json'

            response = s3.get_object(Bucket=bucket, Key=key_path)

            config_json = dict(json.loads(response['Body'].read()))

        common_flow_json = current_flow_json = config_json.get('common')
        current_flow_json = config_json.get(flow, None)

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
