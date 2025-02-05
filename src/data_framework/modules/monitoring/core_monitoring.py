from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.monitoring.interface_monitoring import (
    MonitoringInterface,
    Metric,
    MetricNames
)
from typing import Union

class CoreMonitoring:

    @LazyClassProperty
    def _monitoring(cls) -> MonitoringInterface:
        from data_framework.modules.monitoring.integrations.aws_cloudwatch.aws_cloudwatch_monitoring import AWSCloudWatch
        return AWSCloudWatch()

    @classmethod
    def track_metric(
        cls,
        metric: Metric
    ):
        cls._monitoring.track_metric(metric=metric)
    
    @classmethod
    def track_table_metric(
        cls,
        name: MetricNames,
        database: str,
        table: str,
        value: float
    ):
        cls._monitoring.track_table_metric(
            name=name,
            database=database,
            table=table,
            value=value
        )
    
    @classmethod
    def track_process_metric(
        cls,
        name: MetricNames,
        value: float,
        success: bool = None
    ):
        cls._monitoring.track_process_metric(name=name, value=value, success=success)

    @classmethod
    def track_computation_metric(
        cls,
        name: MetricNames,
        value: float
    ):
        cls._monitoring.track_computation_metric(name=name, value=value)