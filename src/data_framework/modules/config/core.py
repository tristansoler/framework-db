from typing import Type, TypeVar, Union, get_type_hints, get_origin, get_args
from data_framework.modules.config.model.flows import (
    Processes,
    LandingToRaw,
    ToOutput,
    IncomingFileLandingToRaw,
    DateLocatedFilename,
    Parameters,
    CSVSpecs,
    Config,
    DatabaseTable,
    ProcessingSpecifications,
    Hardware,
    SparkConfiguration,
    CustomConfiguration,
    OutputReport,
    GenericProcess,
    TableDict,
    CSVSpecsReport,
    VolumetricExpectation,
    Platform,
    Notification,
    NotificationDict
)
import threading
import os
import sys
from enum import Enum
import json
from pathlib import Path
import zipfile
import boto3

T = TypeVar('T')


def config(parameters: dict = None, reset: bool = False) -> Config:
    if ConfigSetup._instancia is None or reset:
        return ConfigSetup(parameters)._instancia.config
    else:
        return ConfigSetup._instancia.config


class ConfigSetup:

    _instancia = None
    _lock = threading.Lock()

    _models = (
        Processes, LandingToRaw, GenericProcess, ToOutput, CSVSpecs, IncomingFileLandingToRaw,
        DateLocatedFilename, DatabaseTable, ProcessingSpecifications,
        Hardware, SparkConfiguration, CustomConfiguration, OutputReport, CSVSpecsReport,
        VolumetricExpectation, Notification
    )

    def __new__(cls, *args, **kwargs):
        if cls._instancia is None:
            with cls._lock:
                if cls._instancia is None:
                    cls._instancia = super(ConfigSetup, cls).__new__(cls)
        return cls._instancia

    def __init__(self, parameters: dict = None):
        try:
            if not parameters:
                parameters = {}
                for i in range(1, len(sys.argv), 2):
                    key = sys.argv[i].replace('--', '').replace('-', '_')
                    value = sys.argv[i+1]
                    parameters[key] = value

            data_framework_config = ConfigSetup.read_data_framework_config()
            parameters['bucket_prefix'] = data_framework_config['s3_bucket_prefix']

            dataflow_config = ConfigSetup.read_dataflow_config(
                dataflow=parameters.get('dataflow'),
                local_file=parameters.get('local_file'),
                environment=data_framework_config['environment'],
                platform=data_framework_config.get('platform', Platform.DATA_PLATFORM.value)
            )

            self._instancia.config = ConfigSetup.parse_to_model(
                model=Config,
                parameters=parameters,
                json_file=dataflow_config
            )
        except Exception as e:
            self._instancia.config = None
            raise RuntimeError(f'Error initializing Data Framework config: {e}')

    @classmethod
    def read_data_framework_config(cls) -> dict:
        # Obtain AWS account ID
        try:
            sts_client = boto3.client('sts', region_name=os.environ["AWS_REGION"])
            sts_client = boto3.client(
                'sts',
                region_name=os.environ["AWS_REGION"],
                endpoint_url=f'https://sts.{os.environ["AWS_REGION"]}.amazonaws.com'
            )
            response = sts_client.get_caller_identity()
            account_id = response['Account']
        except Exception as e:
            raise RuntimeError(f'Error obtaining AWS account ID for config setup: {e}')
        # Read data framework config file
        path_absolute = Path(__file__).resolve()
        if 'data_framework.zip' in path_absolute.parts:
            zip_index = path_absolute.parts.index('data_framework.zip')
            zip_path = Path(*path_absolute.parts[:zip_index+1])
            with zipfile.ZipFile(zip_path, 'r') as z:
                with z.open('data_framework/data_framework_config.json') as file:
                    config_json = dict(json.loads(file.read()))
        else:
            file_path = (path_absolute.parent / '../../data_framework_config.json').resolve()
            with open(file_path) as file:
                config_json = dict(json.loads(file.read()))
        # Search account ID in config file
        current_config = config_json.get(account_id)
        if not current_config:
            account_ids = ', '.join(current_config.keys())
            raise KeyError(
                f'AWS account ID {account_id} not found in Data Framework config. ' +
                f'Available account IDs: {account_ids}'
            )
        else:
            return current_config

    @classmethod
    def read_dataflow_config(cls, dataflow: str, local_file: str, environment: str, platform: str) -> dict:
        if local_file is not None:
            with open(local_file) as file:
                config_json = dict(json.loads(file.read()))
        else:
            path_absolute = Path(__file__).resolve()
            transformation_path = str(path_absolute.parent.parent.parent.parent.parent) + '/transformation.zip'
            with zipfile.ZipFile(transformation_path, 'r') as z:
                with z.open('transformation.json') as file:
                    config_json = dict(json.loads(file.read()))
        dataflows = config_json.get('dataflows')
        common_flow_json = dataflows.get('default')
        current_flow_json = dataflows.get(dataflow, None)
        if current_flow_json is None:
            current_flow_json = common_flow_json
        else:
            current_flow_json = cls.merged_current_dataflow_with_default(
                current_dataflow=current_flow_json,
                default=common_flow_json
            )
        current_flow_json['environment'] = environment
        current_flow_json['platform'] = platform
        current_flow_json['project_id'] = config_json.get('project_id')
        return current_flow_json

    @classmethod
    def merged_current_dataflow_with_default(cls, current_dataflow: dict, default: dict) -> dict:

        merged = current_dataflow.copy()

        for key, value in default.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = cls.merged_current_dataflow_with_default(merged[key], value)
            elif key in merged and isinstance(merged[key], list) and isinstance(value, list):
                merged[key] = merged[key] + value
            elif merged.get(key) is None:
                merged[key] = value

        return merged

    @classmethod
    def parse_to_model(cls, model: Type[T], json_file: dict, parameters: dict = None) -> T:
        # TODO: refactorizar
        fieldtypes = get_type_hints(model)
        kwargs = {}

        try:
            for field, field_type in fieldtypes.items():
                default_value = None
                if isinstance(field_type, type) and issubclass(field_type, cls._models):
                    if json_file:
                        kwargs[field] = cls.parse_to_model(model=field_type, json_file=json_file.get(field))
                elif isinstance(field_type, type) and issubclass(field_type, Enum):
                    value = json_file.get(field)
                    if value:
                        kwargs[field] = field_type(value)
                    else:
                        kwargs[field] = field_type(getattr(model, field))
                elif isinstance(field_type, type) and issubclass(field_type, (TableDict)) and json_file:
                    tables = {}
                    for table_name, config in json_file.get(field, {}).items():
                        tables[table_name] = cls.parse_to_model(model=DatabaseTable, json_file=config)
                    kwargs[field] = TableDict(tables)
                elif isinstance(field_type, type) and issubclass(field_type, (NotificationDict)) and json_file:
                    notifications = {}
                    for notification_name, config in json_file.get(field, {}).items():
                        notifications[notification_name] = cls.parse_to_model(model=Notification, json_file=config)
                    kwargs[field] = NotificationDict(notifications)
                elif isinstance(field_type, type) and issubclass(field_type, (Parameters)):
                    kwargs[field] = cls.parse_to_model(model=field_type, json_file=parameters)
                elif get_origin(field_type) is Union and any(model in get_args(field_type) for model in cls._models):
                    field_model = [model for model in cls._models if model in get_args(field_type)][0]
                    if json_file.get(field):
                        kwargs[field] = cls.parse_to_model(model=field_model, json_file=json_file.get(field))
                elif get_origin(field_type) is list and any(model in get_args(field_type) for model in cls._models):
                    field_model = [model for model in cls._models if model in get_args(field_type)][0]

                    if json_file and json_file.get(field):
                        kwargs[field] = [
                            cls.parse_to_model(model=field_model, json_file=field_item)
                            for field_item in json_file.get(field)
                        ]
                else:
                    if hasattr(model, field):
                        default_value = getattr(model, field)

                    if json_file:
                        kwargs[field] = json_file.get(field, default_value)
                    else:
                        kwargs[field] = default_value
        except Exception as e:
            import traceback
            expection = type(e).__name__
            error = str(e)
            trace = traceback.format_exc()

            # Imprimir la información de la excepción
            print(
                f"""
                    vars: field:{field} field_type:{field_type} parameters:{parameters} json_file:{json_file} default_value:{default_value}
                    Exception: {expection}
                    Error: {error}
                    Trace:
                    {trace}
                """
            )
        return model(**kwargs)
