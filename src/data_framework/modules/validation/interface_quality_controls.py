from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.core import config
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Union, List, Tuple
from datetime import datetime, date
import pandas as pd
from pyspark.sql import DataFrame


@dataclass
class ControlsResponse:
    success: bool
    table: DatabaseTable
    overall_result: bool
    error: Any = None
    data: Union[DataFrame, None] = None
    rules: Union[DataFrame, None] = None
    results: Union[DataFrame, None] = None


@dataclass
class ControlsTable:
    table_config: DatabaseTable
    mandatory_columns: List[str]

    @classmethod
    def master(cls) -> Any:
        return cls(
            table_config=DatabaseTable(
                database='funds_common',
                table='controls_master_v2',
                primary_keys=['control_master_id']
            ),
            mandatory_columns=[
                "control_master_id",
                "control_level",
                "control_threshold_min",
                "control_threshold_max",
                "control_threshold_type"
            ]
        )

    @classmethod
    def dataset(cls) -> Any:
        return cls(
            table_config=DatabaseTable(
                database='funds_common',
                table='controls_dataset_v2',
                primary_keys=['control_master_id', 'control_table_id']
            ),
            mandatory_columns=[
                "control_master_id",
                "control_table_id",
                "control_layer",
                "control_database",
                "control_table",
                "control_field",
                "control_dataset_details",
                "control_level",
                "control_description",
                "control_algorithm_type",
                "control_threshold_min",
                "control_threshold_max",
                "control_threshold_type",
                "blocking_control_indicator"
            ]
        )

    @classmethod
    def results(cls) -> Any:
        return cls(
            table_config=DatabaseTable(
                database='funds_common',
                table='controls_results_v2',
                primary_keys=[
                    'control_master_id', 'control_table_id',
                    'initial_date', 'end_date', 'data_date'
                ]
            ),
            mandatory_columns=[
                'control_master_id',
                'control_table_id',
                'control_result',
                'control_detail',
                'control_outcome',
                'control_metric_value',
                'data_date',
                'initial_date',
                'end_date'
            ]
        )

    @staticmethod
    def filter_rules(layer: Layer, table_config: DatabaseTable) -> str:
        return f"""
            control_database = '{table_config.database}' AND
            control_table = '{table_config.table}' AND
            control_layer = '{layer.value}' AND
            active_control_indicator
        """


@dataclass
class ControlOutcome:
    total: float = 0.0
    value: float = 0.0

    @property
    def metric_value(self) -> Union[float, None]:
        if self.total is not None and self.value is not None:
            metric_value = (
                self.value * 100 / self.total
                if self.total != 0 else 0
            )
            return metric_value

    def to_string(self) -> str:
        return f'{{total: {self.total}, value: {self.value}}}'


@dataclass
class ControlResult:
    master_id: str
    table_id: str
    result_flag: bool = False
    outcome: ControlOutcome = ControlOutcome()
    detail: str = ''
    initial_date: date = date.today()
    end_date: date = date.today()
    data_date: date = date.today()

    def to_series(self) -> pd.Series:
        result = pd.Series({
            'control_master_id': self.master_id,
            'control_table_id': self.table_id,
            'control_result': self.result_flag,
            'control_outcome': self.outcome.to_string(),
            'control_metric_value': self.outcome.metric_value,
            'control_detail': self.detail,
            'initial_date': self.initial_date,
            'end_date': self.end_date,
            'data_date': self.data_date,
        })
        return result

    def set_data_date(self, custom_data_date: datetime = None) -> None:
        if not custom_data_date:
            file_date = config().parameters.file_date
            if file_date is not None:
                self.data_date = datetime.strptime(file_date, '%Y-%m-%d')
        else:
            self.data_date = datetime.strptime(custom_data_date, '%Y-%m-%d')

    def add_detail(self, detail: str, separator: str = '. ') -> None:
        if not self.detail:
            self.detail = detail.strip()
        else:
            self.detail = self.detail + separator + detail.strip()


class ThresholdType(Enum):
    PERCENTAGE = "Percentage"
    ABSOLUTE = "Absolute"
    COUNT = "Count"
    BINARY = "Binary"

    @classmethod
    def available_threshold_types(cls) -> List[str]:
        return [item.value for item in cls]


@dataclass
class ControlThreshold:
    threshold_type: str
    threshold_max: Union[float, None]
    threshold_min: Union[float, None]

    def validate(self) -> None:
        threshold_types = ThresholdType.available_threshold_types()
        if not self.threshold_type:
            raise ValueError(
                'Undefined threshold type. ' +
                f'Available threshold types: {", ".join(threshold_types)}'
            )
        if self.threshold_type not in threshold_types:
            raise ValueError(
                f'Invalid threshold type: {self.threshold_type}. ' +
                f'Available threshold types: {", ".join(threshold_types)}'
            )
        if (
            self.threshold_min is not None and
            self.threshold_max is not None and
            self.threshold_min > self.threshold_max
        ):
            raise ValueError(
                'Invalid threshold limits. Min threshold is greater than max threshold: ' +
                f'{self.threshold_min} > {self.threshold_max}'
            )
        if (
            self.threshold_min is not None and
            self.threshold_max is not None and
            self.threshold_type == ThresholdType.BINARY.value
        ):
            raise ValueError(
                'Invalid threshold limits. Binary threshold does not need threshold limits'
            )

    def apply_threshold(self, results: list) -> Tuple[float, bool]:
        result_value = self.apply_threshold_type(results)
        result_flag = self.apply_threshold_limits(result_value)
        return (result_value, result_flag)

    def apply_threshold_type(self, results: list) -> Union[float, None]:
        if (
            not results and
            self.threshold_type in [ThresholdType.PERCENTAGE.value, ThresholdType.ABSOLUTE.value]
        ):
            raise ValueError(
                f'Cannot apply threshold of type {self.threshold_type} on empty results'
            )
        elif self.threshold_type == ThresholdType.COUNT.value:
            # Number of records
            return len(results)
        elif self.threshold_type == ThresholdType.PERCENTAGE.value:
            # First value expressed as a percentage
            return results[0] * 100
        elif self.threshold_type == ThresholdType.ABSOLUTE.value:
            # First value in absolute value
            return abs(results[0])
        elif self.threshold_type == ThresholdType.BINARY.value:
            # Number of records that are False
            return results.count(False)

    def apply_threshold_limits(self, value: float) -> bool:
        if self.threshold_type == ThresholdType.BINARY.value:
            return value == 0
        elif self.threshold_min is not None and self.threshold_max is not None:
            return (self.threshold_min <= value <= self.threshold_max)
        elif self.threshold_min is not None:
            return (self.threshold_min <= value)
        elif self.threshold_max is not None:
            return (value <= self.threshold_max)
        else:
            return True


class AlgorithmType(Enum):
    PYTHON = "Python"
    SQL = "SQL"

    @classmethod
    def available_algorithm_types(cls) -> List[str]:
        return [item.value for item in cls]


class ControlLevel(Enum):
    DATA = "Data"
    FILE = "File"

    @classmethod
    def available_control_levels(cls) -> List[str]:
        return [item.value for item in cls]


@dataclass
class ControlAlgorithm:
    algorithm_type: str
    algorithm_description: str

    def validate(self) -> None:
        algorithm_types = AlgorithmType.available_algorithm_types()
        if not self.algorithm_type:
            raise ValueError(
                'Undefined algorithm type. ' +
                f'Available algorithm types: {", ".join(algorithm_types)}'
            )
        if self.algorithm_type not in algorithm_types:
            raise ValueError(
                f'Invalid algorithm type: {self.algorithm_type}. ' +
                f'Available algorithm types: {", ".join(algorithm_types)}'
            )
        if not self.algorithm_description:
            raise ValueError('Undefined algorithm description')


@dataclass
class ControlRule:
    master_id: str
    table_id: str
    layer: str
    database_table: DatabaseTable
    field: str
    rule_description: str
    level: str
    algorithm: ControlAlgorithm
    threshold: ControlThreshold
    is_blocker: bool
    result: ControlResult

    @property
    def id(self) -> str:
        return f"{self.master_id}:{self.table_id}"

    @classmethod
    def from_series(cls, rule: pd.Series) -> Any:
        return cls(
            master_id=rule['control_master_id'],
            table_id=rule['control_table_id'],
            layer=rule['control_layer'],
            database_table=DatabaseTable(
                database=rule['control_database'],
                table=rule['control_table']
            ),
            field=rule['control_field'],
            rule_description=rule['control_dataset_details'],
            level=rule['control_level'],
            algorithm=ControlAlgorithm(
                algorithm_type=rule['control_algorithm_type'],
                algorithm_description=rule['control_description']
            ),
            threshold=ControlThreshold(
                threshold_type=rule['control_threshold_type'],
                threshold_max=(
                    None if pd.isna(rule['control_threshold_max'])
                    else rule['control_threshold_max']
                ),
                threshold_min=(
                    None if pd.isna(rule['control_threshold_min'])
                    else rule['control_threshold_min']
                )
            ),
            is_blocker=rule['blocking_control_indicator'],
            result=ControlResult(
                master_id=rule['control_master_id'],
                table_id=rule['control_table_id']
            )
        )

    def validate(self) -> None:
        # TODO: add more validations
        self.threshold.validate()
        self.algorithm.validate()
        control_levels = ControlLevel.available_control_levels()
        if not self.level:
            raise ValueError(
                'Undefined control level. ' +
                f'Available control levels: {", ".join(control_levels)}'
            )
        if self.level not in control_levels:
            raise ValueError(
                f'Invalid control level: {self.level}. ' +
                f'Available control levels: {", ".join(control_levels)}'
            )

    def calculate_result(self, results: list, total_records: int = None) -> None:
        result_value, result_flag = self.threshold.apply_threshold(results)
        self.result.result_flag = result_flag
        if total_records is None:
            total_records = len(results)
        self.result.outcome = ControlOutcome(
            total=total_records,
            value=result_value
        )


class InterfaceQualityControls(ABC):

    @abstractmethod
    def validate(self, layer: Layer, table_config: DatabaseTable, df_data: DataFrame = None) -> ControlsResponse:
        pass

    @abstractmethod
    def set_parent(self, parent: Any) -> None:
        pass
