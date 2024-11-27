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
    CSVSpecsReport
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
        Processes, LandingToRaw, GenericProcess, ToOutput, CSVSpecs, IncomingFileLandingToRaw,
        DateLocatedFilename, DatabaseTable, ProcessingSpecifications,
        Hardware, SparkConfiguration, CustomConfiguration, OutputReport, CSVSpecsReport
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

        local_file = parameters.get('local_file')
        dataflow = parameters.get('dataflow')
        json_config = ConfigSetup.read_config_file(dataflow=dataflow, local_file=local_file)

        self._instancia.config = ConfigSetup.parse_to_model(model=Config, parameters=parameters, json_file=json_config)

    @classmethod
    def read_config_file(cls, dataflow: str, local_file: str) -> dict:
        import json
        from pathlib import Path

        config_json = {}

        if local_file is not None:
            file = open(local_file)
            config_json = dict(json.loads(file.read()))
        else:
            import zipfile

            path_absolute = Path(__file__).resolve()
            transformation_path = str(path_absolute.parent.parent.parent.parent.parent) + '/transformation.zip'
            archive = zipfile.ZipFile(transformation_path, 'r')
            config_file = archive.open('transformation.json')
            config_json = dict(json.loads(config_file.read()))
            config_file.close()

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
        current_flow_json['environment'] = "develop"
        current_flow_json['project_id'] = config_json.get('project_id')

        return current_flow_json

    @classmethod
    def merged_current_dataflow_with_default(cls, current_dataflow: dict, default: dict) -> dict:

        merged = current_dataflow.copy()

        for key, value in default.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = cls.merged_current_dataflow_with_default(merged[key], value)
            else:
                if merged.get(key) is None:
                    merged[key] = value

        return merged

    @classmethod
    def parse_to_model(cls, model: Type[T], json_file: dict, parameters: dict = None) -> T:
        # TODO: refactorizar
        fieldtypes = get_type_hints(model)
        kwargs = {}

        try:
            for field, field_type in fieldtypes.items():
                if isinstance(field_type, type) and issubclass(field_type, cls._models):
                    if json_file:
                        kwargs[field] = cls.parse_to_model(model=field_type, json_file=json_file.get(field))
                elif isinstance(field_type, type) and issubclass(field_type, (TableDict)) and json_file:
                    tables = {}
                    for table_name, config in json_file.get(field, {}).items():
                        tables[table_name] = cls.parse_to_model(model=DatabaseTable, json_file=config)
                    kwargs[field] = TableDict(tables)
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
                    default_value = None
                    if hasattr(model, field):
                        default_value = getattr(model, field)
                    kwargs[field] = json_file.get(field, default_value)
        except Exception as e:
            import traceback
            expection = type(e).__name__
            error = str(e)
            trace = traceback.format_exc()

            # Imprimir la información de la excepción
            print(
                f"""
                    Exception: {expection}
                    Error: {error}
                    Trace:
                    {trace}
                """
            )
        return model(**kwargs)
