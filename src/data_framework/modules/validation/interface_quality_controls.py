from data_framework.modules.config.model.flows import DatabaseTable, Database, Technologies, Platform
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.config.core import config
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Union, List
from datetime import datetime, date
import pandas as pd


@dataclass
class ControlsResponse:
    success: bool
    table: DatabaseTable
    overall_result: bool
    error: Any = None
    data: Any = None
    rules: Any = None
    results: Any = None


@dataclass
class ControlsTable:
    table_config: DatabaseTable
    mandatory_columns: List[str]

    @classmethod
    def master(cls) -> Any:
        # TODO: remove when migrating Infinity to Data Platform
        database = (
            Database.INFINITY_COMMON if config().platform == Platform.INFINITY
            else Database.DATA_QUALITY
        )
        return cls(
            table_config=DatabaseTable(
                database=database,
                table='controls_master',
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
        # TODO: remove when migrating Infinity to Data Platform
        database = (
            Database.INFINITY_COMMON if config().platform == Platform.INFINITY
            else Database.DATA_QUALITY
        )
        return cls(
            table_config=DatabaseTable(
                database=database,
                table='controls_dataset',
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
                "control_threshold_rag_min",
                "control_threshold_rag_max",
                "control_threshold_type",
                "blocking_control_indicator"
            ]
        )

    @classmethod
    def results(cls) -> Any:
        # TODO: remove when migrating Infinity to Data Platform
        database = (
            Database.INFINITY_COMMON if config().platform == Platform.INFINITY
            else Database.DATA_QUALITY
        )
        return cls(
            table_config=DatabaseTable(
                database=database,
                table='controls_results',
                primary_keys=[
                    'control_master_id', 'control_table_id',
                    'initial_date', 'end_date', 'data_date'
                ]
            ),
            mandatory_columns=[
                'control_master_id',
                'control_table_id',
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
            control_database = '{table_config.database.value}' AND
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
            metric_value = 1.0 - (
                self.value / self.total
                if self.total != 0 else 0
            )
            return metric_value

    def to_string(self) -> str:
        return f'{{total: {self.total}, value: {self.value}}}'


@dataclass
class ThresholdResult:
    total_records: int
    invalid_records: int
    valid_identifiers: List[str]
    invalid_identifiers: List[str]

    @property
    def unique_valid_identifiers(self) -> List[str]:
        return list(set(self.valid_identifiers))

    @property
    def unique_invalid_identifiers(self) -> List[str]:
        return list(set(self.invalid_identifiers))


@dataclass
class ControlResult:
    master_id: str
    table_id: str
    outcome: ControlOutcome = field(default_factory=ControlOutcome)
    detail: str = ''
    initial_date: date = datetime.now()
    end_date: date = None
    data_date: date = date.today()

    def to_series(self) -> pd.Series:
        result = pd.Series({
            'control_master_id': self.master_id,
            'control_table_id': self.table_id,
            'control_outcome': self.outcome.to_string(),
            'control_metric_value': self.outcome.metric_value,
            'control_detail': self.detail,
            'initial_date': self.initial_date,
            'end_date': datetime.now(),
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

    def fill(self, threshold_result: ThresholdResult) -> None:
        self.outcome = ControlOutcome(
            total=threshold_result.total_records,
            value=threshold_result.invalid_records
        )


class ThresholdType(Enum):
    STANDARD = "Standard"
    ABSOLUTE = "Absolute"
    BINARY = "Binary"

    @classmethod
    def available_threshold_types(cls) -> List[str]:
        return [item.value for item in cls]


@dataclass
class ControlThreshold:
    threshold_type: str
    threshold_max: Union[float, None]
    threshold_min: Union[float, None]
    threshold_rag_min: Union[float, None]
    threshold_rag_max: Union[float, None]

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
            self.threshold_rag_min is not None and
            self.threshold_rag_max is not None and
            self.threshold_rag_min > self.threshold_rag_max
        ):
            raise ValueError(
                'Invalid threshold percentages. Min threshold is greater than max threshold: ' +
                f'{self.threshold_rag_min} > {self.threshold_rag_max}'
            )
        if (
            self.threshold_rag_min is not None and not
            0.0 <= self.threshold_rag_min <= 1.0
        ):
            raise ValueError(
                'Invalid min threshold percentage. Must be expressed between 0.0 and 1.0: ' +
                f'{self.threshold_rag_min}'
            )
        if (
            self.threshold_rag_max is not None and not
            0.0 <= self.threshold_rag_max <= 1.0
        ):
            raise ValueError(
                'Invalid max threshold percentage. Must be expressed between 0.0 and 1.0: ' +
                f'{self.threshold_rag_max}'
            )
        if (
            self.threshold_min is not None and
            self.threshold_max is not None and
            self.threshold_type == ThresholdType.BINARY.value
        ):
            raise ValueError(
                f'Invalid threshold limits. {ThresholdType.BINARY.value} ' +
                'threshold does not need threshold limits'
            )

    def apply_threshold(self, df_result: Any) -> ThresholdResult:
        if (
            df_result is None or
            'identifier' not in df_result.columns or
            'result' not in df_result.columns
        ):
            raise ValueError(
                'The DataFrame with the results on which to apply the threshold ' +
                'must have the columns "identifier" and "result"'
            )
        if self.threshold_type == ThresholdType.STANDARD.value:
            return self.calculate_standard_threshold(df_result)
        elif self.threshold_type == ThresholdType.ABSOLUTE.value:
            return self.calculate_absolute_threshold(df_result)
        elif self.threshold_type == ThresholdType.BINARY.value:
            return self.calculate_binary_threshold(df_result)

    def calculate_standard_threshold(self, df_result: Any) -> ThresholdResult:
        technology = config().current_process_config().processing_specifications.technology
        if technology == Technologies.EMR:
            from pyspark.sql.functions import col, udf
            from pyspark.sql.types import BooleanType
            # Apply threshold to each record
            udf_function = udf(self.apply_threshold_limits, BooleanType())
            df_result = df_result.withColumn('result_flag', udf_function(col('result')))
            # Records with True result
            valid_ids = df_result.filter(col('result_flag')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Records with False result
            invalid_ids = df_result.filter(~col('result_flag')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Calculate threshold
            total_records = df_result.count()
            invalid_records = len(invalid_ids)
        else:
            # Apply threshold to each record
            df_result['result_flag'] = df_result['result'].apply(self.apply_threshold_limits)
            # Records with True result
            valid_ids = df_result[df_result['result_flag']]['identifier'].tolist()
            # Records with False result
            invalid_ids = df_result[~df_result['result_flag']]['identifier'].tolist()
            # Calculate threshold
            total_records = len(df_result)
            invalid_records = len(invalid_ids)
        # Build response
        result = ThresholdResult(
            total_records=total_records,
            invalid_records=invalid_records,
            valid_identifiers=valid_ids,
            invalid_identifiers=invalid_ids
        )
        return result

    def calculate_absolute_threshold(self, df_result: Any) -> ThresholdResult:
        technology = config().current_process_config().processing_specifications.technology
        if technology == Technologies.EMR:
            from pyspark.sql.functions import col, udf, abs
            from pyspark.sql.types import BooleanType
            # Apply threshold and absolute value to each record
            udf_function = udf(self.apply_threshold_limits, BooleanType())
            df_result = df_result.withColumn('result_flag', udf_function(abs(col('result'))))
            # Records with True result
            valid_ids = df_result.filter(col('result_flag')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Records with False result
            invalid_ids = df_result.filter(~col('result_flag')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Calculate threshold
            total_records = df_result.count()
            invalid_records = len(invalid_ids)
        else:
            # Apply threshold and absolute value to each record
            df_result['result_flag'] = df_result['result'].abs().apply(self.apply_threshold_limits)
            # Records with True result
            valid_ids = df_result[df_result['result_flag']]['identifier'].tolist()
            # Records with False result
            invalid_ids = df_result[~df_result['result_flag']]['identifier'].tolist()
            # Calculate threshold
            total_records = len(df_result)
            invalid_records = len(invalid_ids)
        # Build response
        result = ThresholdResult(
            total_records=total_records,
            invalid_records=invalid_records,
            valid_identifiers=valid_ids,
            invalid_identifiers=invalid_ids
        )
        return result

    def calculate_binary_threshold(self, df_result: Any) -> ThresholdResult:
        technology = config().current_process_config().processing_specifications.technology
        if technology == Technologies.EMR:
            from pyspark.sql.functions import col
            # Records with True result
            valid_ids = df_result.filter(col('result')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Records with False result
            invalid_ids = df_result.filter(~col('result')).select('identifier').rdd.flatMap(lambda x: x).collect()
            # Calculate threshold
            total_records = df_result.count()
            invalid_records = len(invalid_ids)
        else:
            # Records with True result
            valid_ids = df_result[df_result['result']]['identifier'].tolist()
            # Records with False result
            invalid_ids = df_result[~df_result['result']]['identifier'].tolist()
            # Calculate threshold
            total_records = len(df_result)
            invalid_records = len(invalid_ids)
        # Build response
        result = ThresholdResult(
            total_records=total_records,
            invalid_records=invalid_records,
            valid_identifiers=valid_ids,
            invalid_identifiers=invalid_ids
        )
        return result

    def apply_threshold_limits(self, value: float) -> bool:
        if self.threshold_min is not None and self.threshold_max is not None:
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
    REGEX = "Regex"

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

    @property
    def field_list(self) -> List[str]:
        return [column.strip() for column in self.field.split(',')]

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
                ),
                threshold_rag_min=(
                    None if pd.isna(rule['control_threshold_rag_min'])
                    else rule['control_threshold_rag_min']
                ),
                threshold_rag_max=(
                    None if pd.isna(rule['control_threshold_rag_max'])
                    else rule['control_threshold_rag_max']
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

    def calculate_result(self, df_result: Any) -> ThresholdResult:
        threshold_result = self.threshold.apply_threshold(df_result)
        self.result.fill(threshold_result)
        return threshold_result


class InterfaceQualityControls(ABC):

    @abstractmethod
    def validate(self, layer: Layer, table_config: DatabaseTable, df_data: Any = None) -> ControlsResponse:
        pass

    @abstractmethod
    def set_parent(self, parent: Any) -> None:
        pass
