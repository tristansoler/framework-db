from data_framework.modules.monitoring.interface_monitoring import (
    MonitoringInterface,
    Metric,
    MetricNames,
    MetricUnits
)
from typing import (
    Union,
    Optional,
    List, Dict, Any
)
from dataclasses import dataclass
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
import datetime
import boto3
import os
import boto3.data.cloudwatch
from botocore.client import BaseClient

@dataclass
class InternalMetric(Metric):

    @property
    def parse(self) -> Dict:
        return {
            "MetricName": self.name.value,
            "Dimensions": self.dimensions,
            "Timestamp": self._created_at,
            "Value": self.value,
            "Unit": self.name.unit.value
        }
    
class AWSCloudWatch(MonitoringInterface):

    __namespace = "DataFramework/Product"
    __computation_dimensions = [
        {
            'Name': 'ExecutionId', 'Value': 'KjNDxQ'
        },
        {
            'Name': 'ProjectId', 'Value': config().project_id
        },
        {
            'Name': 'DataFlow', 'Value': config().parameters.dataflow
        },
        {
            'Name': 'Process', 'Value': config().parameters.process
        },
        { # TODO: Move to another class (? Cloud -> AWS Parameters?)
            'Name': 'JobId', 'Value': os.environ.get("EMR_SERVERLESS_JOB_ID", "Unknown")
        }
    ]

    __process_dimensions = [
        {
            'Name': 'ExecutionId', 'Value': 'KjNDxQ'
        },
        {
            'Name': 'ProjectId', 'Value': config().project_id
        },
        {
            'Name': 'DataFlow', 'Value': config().parameters.dataflow
        },
        {
            'Name': 'Process', 'Value': config().parameters.process
        },
        { # TODO: Move to another class (? Cloud -> AWS Parameters?)
            'Name': 'JobId', 'Value': os.environ.get("EMR_SERVERLESS_JOB_ID", "Unknown")
        }
    ]

    __table_dimensions = [
        {
            'Name': 'ExecutionId', 'Value': 'KjNDxQ'
        },
        {
            'Name': 'ProjectId', 'Value': config().project_id
        },
        {
            'Name': 'DataFlow', 'Value': config().parameters.dataflow
        },
        {
            'Name': 'Process', 'Value': config().parameters.process
        },
        { # TODO: Move to another class (? Cloud -> AWS Parameters?)
            'Name': 'JobId', 'Value': os.environ.get("EMR_SERVERLESS_JOB_RUN_ID", "Unknown")
        }
    ]

    @property
    def namespace(self) -> str:
        return self.__namespace

    @property
    def client(self) -> BaseClient:
        self._client

    def __init__(self):
        self._client = boto3.client(
            'cloudwatch',
            region_name=config().parameters.region,
            endpoint_url=f'https://monitoring.{os.environ["AWS_REGION"]}.amazonaws.com'
        )

    def track_metric(self, metric: InternalMetric):

        response = self._client.put_metric_data(
            Namespace=self.namespace,
            MetricData=[
                metric.parse
            ]
        )

    def track_table_metric(
        self,
        name: MetricNames,
        database: str,
        table: str,
        value: float
    ):
        dimenions = self.__table_dimensions
        
        dimenions = dimenions + [
            {'Name': 'Database', 'Value': database},
            {'Name': 'Table', 'Value': table}
        ]

        metric = InternalMetric(
            name=name,
            value=value,
            dimensions=dimenions
        )

        self.track_metric(metric=metric)
        
    
    def track_process_metric(self, name: MetricNames, value: float, success: bool = None):
        dimenions = self.__process_dimensions
        if success:
            dimenions.append({'Name': 'Success', 'Value': str(success).lower()})

        metric = InternalMetric(
            name=name,
            value=value,
            dimensions=dimenions
        )

        self.track_metric(metric=metric)

    def track_computation_metric(self, name: MetricNames, value: float):
        metric = InternalMetric(
            name=name,
            value=value,
            dimensions=self.__computation_dimensions
        )

        self.track_metric(metric=metric)