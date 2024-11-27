from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.validation.interface_quality_controls import InterfaceQualityControls, ControlsResponse
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.storage.interface_storage import Layer
from pyspark.sql import DataFrame
from typing import Any


class CoreQualityControls(object):

    @LazyClassProperty
    def _quality_controls(cls) -> InterfaceQualityControls:
        from data_framework.modules.validation.integrations.quality_controls import QualityControls
        return QualityControls()

    @classmethod
    def validate(cls, layer: Layer, table_config: DatabaseTable, df_data: DataFrame = None) -> ControlsResponse:
        return cls._quality_controls.validate(layer=layer, table_config=table_config, df_data=df_data)

    @classmethod
    def set_parent(cls, parent: Any) -> None:
        return cls._quality_controls.set_parent(parent=parent)
