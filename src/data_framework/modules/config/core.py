from typing import Type, TypeVar, Union, get_type_hints, get_origin, get_args
from data_framework.modules.config.model.flows import (
    Processes,
    LandingToRaw,
    ToOutput,
    IncomingFileLandingToRaw,
    DateLocatedFilename,
    Parameters,
    CSVSpecs,
    XMLSpecs,
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
    JSONSpecsReport,
    VolumetricExpectation,
    Platform,
    Technologies,
    Environment,
    ProcessVars,
    Casting,
    Transformation
)
from data_framework.modules.notification.interface_notifications import (
    NotificationDict,
    DataFrameworkNotifications,
    Notification,
    NotificationsParameters
)
from data_framework.modules.exception.config_exceptions import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigParseError,
    AccountNotFoundError,
    ParameterParseError
)
from data_framework.modules.exception.aws_exceptions import STSError
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
    _environment: None

    _models = (
        Processes, LandingToRaw, GenericProcess, ToOutput, CSVSpecs, XMLSpecs, IncomingFileLandingToRaw,
        DateLocatedFilename, DatabaseTable, ProcessingSpecifications,
        Hardware, SparkConfiguration,
        OutputReport, CSVSpecsReport, JSONSpecsReport,
        VolumetricExpectation, Notification, Casting, Transformation,
        DataFrameworkNotifications, NotificationsParameters
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
                try:
                    for i in range(1, len(sys.argv), 2):
                        key = sys.argv[i].replace('--', '').replace('-', '_')
                        value = sys.argv[i+1]
                        parameters[key] = value
                except Exception:
                    raise ParameterParseError(arguments=sys.argv)

            data_framework_config = ConfigSetup.read_data_framework_config()
            parameters['bucket_prefix'] = data_framework_config['s3_bucket_prefix']
            platform = data_framework_config.get('platform', Platform.DATA_PLATFORM.value)
            if platform == Platform.INFINITY.value and not parameters.get('local_file'):
                # Custom configurarion for Infinity execution
                # TODO: remove when migrating to Data Platform
                parameters['dataflow'] = 'default'
                parameters['process'] = 'landing_to_raw'
                self._instancia.config = Config(
                    processes=Processes(
                        landing_to_raw=LandingToRaw(
                            incoming_file=None,
                            output_file=None,
                            processing_specifications=ProcessingSpecifications(
                                technology=Technologies.LAMBDA,
                            )
                        )
                    ),
                    environment=Environment(data_framework_config['environment']),
                    platform=Platform.INFINITY,
                    parameters=ConfigSetup.parse_to_model(
                        model=Parameters,
                        json_file=parameters
                    ),
                    project_id=Platform.INFINITY.value
                )
            else:
                dataflow_config = ConfigSetup.read_dataflow_config(
                    dataflow=parameters.get('dataflow'),
                    local_file=parameters.get('local_file'),
                    environment=data_framework_config['environment'],
                    platform=platform
                )

                self._instancia.config = ConfigSetup.parse_to_model(
                    model=Config,
                    parameters=parameters,
                    json_file=dataflow_config,
                    environment=data_framework_config['environment']
                )
        except Exception:
            self._instancia.config = None
            raise ConfigError()

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
        except Exception:
            raise STSError(error_message='Error obtaining AWS account ID from STS for config setup')
        # Read data framework config file
        config_json = cls.read_config_file(
            absolute_path='data_framework/modules/config/data_framework_config.json',
            relative_path='data_framework_config.json'
        )
        # Search account ID in config file
        current_config = config_json.get(account_id)
        if not current_config:
            raise AccountNotFoundError(
                account_id=account_id, available_ids=list(config_json.keys())
            )
        else:
            return current_config

    @classmethod
    def read_notifications_config(cls) -> dict:
        # Read data framework notifications file
        notifications_config = cls.read_config_file(
            absolute_path='data_framework/modules/notification/data_framework_notifications.json',
            relative_path='../notification/data_framework_notifications.json'
        )
        return notifications_config

    @classmethod
    def read_config_file(cls, absolute_path: str, relative_path: str) -> dict:
        try:
            current_path = Path(__file__).resolve()
            if 'data_framework.zip' in current_path.parts:
                config_path = absolute_path
                zip_index = current_path.parts.index('data_framework.zip')
                zip_path = Path(*current_path.parts[:zip_index+1])
                with zipfile.ZipFile(zip_path, 'r') as z:
                    with z.open(config_path) as file:
                        config_json = dict(json.loads(file.read()))
            else:
                config_path = (current_path.parent / relative_path).resolve()
                with open(config_path) as file:
                    config_json = dict(json.loads(file.read()))
            return config_json
        except FileNotFoundError:
            raise ConfigFileNotFoundError(config_file_path=config_path)

    @classmethod
    def read_dataflow_config(cls, dataflow: str, local_file: str, environment: str, platform: str) -> dict:
        try:
            if local_file is not None:
                transformation_path = local_file
                with open(transformation_path) as file:
                    config_json = dict(json.loads(file.read()))
            else:
                path_absolute = Path(__file__).resolve()
                transformation_path = str(path_absolute.parent.parent.parent.parent.parent) + '/transformation.zip'
                with zipfile.ZipFile(transformation_path, 'r') as z:
                    with z.open('transformation.json') as file:
                        config_json = dict(json.loads(file.read()))
        except Exception:
            raise ConfigFileNotFoundError(config_file_path=transformation_path)
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
        current_flow_json['data_framework_notifications'] = cls.read_notifications_config()
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
    def parse_to_model(cls, model: Type[T], json_file: dict, environment: str, parameters: dict = None) -> T:
        # TODO: refactorizar
        fieldtypes = get_type_hints(model)
        kwargs = {}
        model_instantiated = None

        try:
            for field, field_type in fieldtypes.items():
                default_value = None
                if isinstance(field_type, type) and issubclass(field_type, cls._models) and json_file:
                    kwargs[field] = cls.parse_to_model(
                        model=field_type,
                        json_file=json_file.get(field),
                        environment=environment
                    )
                elif isinstance(field_type, type) and issubclass(field_type, Enum) and json_file:
                    value = json_file.get(field)
                    if value:
                        kwargs[field] = field_type(value)
                    else:
                        kwargs[field] = field_type(getattr(model, field))
                elif isinstance(field_type, type) and issubclass(field_type, (TableDict)) and json_file:
                    tables = {
                        table_name: cls.parse_to_model(
                            model=DatabaseTable,
                            json_file=config,
                            environment=environment
                        )
                        for table_name, config in json_file.get(field, {}).items()
                    }
                    kwargs[field] = TableDict(tables)
                elif isinstance(field_type, type) and issubclass(field_type, (NotificationDict)) and json_file:
                    notifications = {
                        notification_name: cls.parse_to_model(
                            model=Notification,
                            json_file=config,
                            environment=environment
                        )
                        for notification_name, config in json_file.get(field, {}).items()
                    }
                    kwargs[field] = NotificationDict(notifications)
                elif isinstance(field_type, type) and issubclass(field_type, (Parameters)):
                    kwargs[field] = cls.parse_to_model(model=field_type, json_file=parameters, environment=environment)
                elif ProcessVars in get_args(field_type):
                    default = {'default': {}, 'develop': {}, 'preproduction': {}, 'production': {}}
                    all_vars = json_file.get(field, default)
                    variables = cls.merged_current_dataflow_with_default(
                        current_dataflow=all_vars.get(environment),
                        default=all_vars.get('default')
                    )

                    kwargs[field] = ProcessVars(_variables=variables)
                elif get_origin(field_type) is Union and any(model in get_args(field_type) for model in cls._models):
                    field_model = [model for model in cls._models if model in get_args(field_type)][0]
                    if json_file.get(field):
                        kwargs[field] = cls.parse_to_model(
                            model=field_model,
                            json_file=json_file.get(field),
                            environment=environment
                        )
                    elif type(None) in get_args(field_type):
                        kwargs[field] = None
                elif get_origin(field_type) is list and any(model in get_args(field_type) for model in cls._models) and json_file:
                    field_model = [model for model in cls._models if model in get_args(field_type)][0]
                    if json_file.get(field):
                        kwargs[field] = [
                            cls.parse_to_model(
                                model=field_model.get_subclass_from_dict(field_item),
                                json_file=field_item,
                                environment=environment
                            )
                            if field_model == Transformation else
                            cls.parse_to_model(model=field_model, json_file=field_item, environment=environment)
                            for field_item in json_file.get(field)
                        ]
                elif get_origin(field_type) is list and all(issubclass(item, Enum) for item in get_args(field_type)) and json_file:
                    kwargs[field] = [get_args(field_type)[0](value) for value in json_file.get(field)]
                else:
                    if hasattr(model, field):
                        default_value = getattr(model, field)
                    elif get_origin(field_type) is list:
                        default_value = []
                    if json_file:
                        kwargs[field] = json_file.get(field, default_value)
                    else:
                        kwargs[field] = default_value
            model_instantiated = model(**kwargs)
        except Exception:
            raise ConfigParseError(field=field, field_type=str(field_type))

        return model_instantiated
