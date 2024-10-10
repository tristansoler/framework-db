from modules.storage.core_storage import Storage
from typing import Any, Type, TypeVar, get_type_hints
from modules.config.versions.v1 import Flows, LandingToRaw, SourceLandingToRaw, Payload, Config
import os
import sys

T = TypeVar('T')

class ConfigSetup:
	@classmethod
	def initialize(cls, payload: dict = None) -> Config:

		if not payload:
			payload = {}
			for i in range(1, len(sys.argv), 2):
				key = sys.argv[i].replace('--', '').replace('-', '_')
				value = sys.argv[i+1]
				payload[key] = value

		# TODO: Pending
		os.getenv('ENV') == 'local'

		json_config = cls.read_config_file(is_local=False)

		config = cls.v1(model=Config, payload=payload, json_file=json_config)
	
		return config
	
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
	def v1(cls, model: Type[T], json_file: dict, payload: dict = None) -> T:
		

		fieldtypes = get_type_hints(model)
		kwargs = {}

		for field, field_type in fieldtypes.items():
			if isinstance(field_type, type) and issubclass(field_type, (Flows, LandingToRaw, SourceLandingToRaw)):
				kwargs[field] = cls.v1(model=field_type, json_file=json_file[field])
			elif isinstance(field_type, type) and issubclass(field_type, (Payload)):
				kwargs[field] = cls.v1(model=field_type, json_file=payload)
			else:
				kwargs[field] = json_file[field]
		return model(**kwargs)