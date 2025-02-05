from abc import ABC, abstractmethod
from typing import (
    Union,
    Optional,
    List, Dict, Any
)
from dataclasses import dataclass, field, fields
from enum import Enum
import time
from data_framework.modules.config.core import config
import os

class MetricUnits(Enum):
    SECONDS = "Seconds"
    MEGABYTES = "Megabytes"
    COUNT = "Count"
    PERCENT = "Percent"
    NONE = "None"

class MetricNames(Enum):
    UNKNOWN = "Unknown"

    TABLE_READ_RECORDS = "TableReadRecords"
    TABLE_READ_SIZE = "TableReadSize"

    TABLE_WRITE_ADDED_RECORDS = "TableWriteAddedRecords"
    TABLE_WRITE_ADDED_SIZE = "TableWriteAddedSize"
    TABLE_WRITE_DELETED_RECORDS = "TableWriteDeletedRecords"
    TABLE_WRITE_DELETED_SIZE = "TableWriteDeletedSize"
    TABLE_WRITE_TOTAL_RECORDS = "TableWriteTotalRecords"
    TABLE_WRITE_TOTAL_SIZE = "TableWriteTotalSize"

    DATAFLOW_START_EVENT = "DataFlowStartEvent"
    DATAFLOW_END_EVENT = "DataFlowEndEvent"

    PROCESS_START_EVENT = "ProcessStartEvent"
    PROCESS_END_EVENT = "ProcessEndEvent"
    PROCESS_DURATION = "ProcessDuration"

    @property
    def unit(self) -> MetricUnits:
        if self.name in [
            self.__class__.TABLE_READ_SIZE.name,
            self.__class__.TABLE_WRITE_ADDED_SIZE.name,
            self.__class__.TABLE_WRITE_DELETED_SIZE.name,
            self.__class__.TABLE_WRITE_TOTAL_SIZE.name
        ]:
            return MetricUnits.MEGABYTES
        
        if self.name in [
            self.__class__.PROCESS_DURATION.name
        ]:
            return MetricUnits.SECONDS
        
        if self.name in [
            self.__class__.TABLE_READ_RECORDS.name,
            self.__class__.TABLE_WRITE_ADDED_RECORDS.name,
            self.__class__.TABLE_WRITE_DELETED_RECORDS.name,
            self.__class__.TABLE_WRITE_TOTAL_RECORDS.name
        ]:
            return MetricUnits.COUNT
        
        if self.name in [
            self.__class__.DATAFLOW_START_EVENT.name,
            self.__class__.DATAFLOW_END_EVENT.name,
            self.__class__.PROCESS_START_EVENT.name,
            self.__class__.PROCESS_END_EVENT.name,
            self.__class__.UNKNOWN.name,
        ]:
            return MetricUnits.NONE
        

@dataclass
class Metric:
    _created_at: str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    name: MetricNames = MetricNames.UNKNOWN
    value: float = 0
    dimensions: List[Dict[str, str]] = field(default_factory=List)

class MonitoringInterface(ABC):

    @abstractmethod
    def track_metric(
        self,
        metric: Metric
    ):
        # Abstract class to define the basic storage interface
        pass

    @abstractmethod
    def track_table_metric(
        self,
        name: MetricNames,
        database: str,
        table: str,
        value: float
    ):
        pass

    @abstractmethod
    def track_process_metric(
        self,
        name: MetricNames,
        value: float,
        success: bool = None
    ):
        pass

    @abstractmethod
    def track_computation_metric(
        self,
        name: MetricNames,
        value: float
    ):
        pass
