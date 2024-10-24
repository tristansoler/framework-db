from data_framework.modules.code.lazy_class_property import LazyClassProperty
from data_framework.modules.data_process.interface_data_process import DataProcessInterface
from data_framework.modules.config.core import config
from data_framework.modules.config.model.flows import Technologies


class CoreDataProcess(object):

    @LazyClassProperty
    def _data_process(cls) -> DataProcessInterface:
        process = config().parameters.process
        technology = getattr(config().processes, process) \
            .processing_specifications.technology
        if technology == Technologies.EMR.value:
            from data_framework.modules.data_process.spark_data_process import SparkDataProcess
            return SparkDataProcess()
        elif technology == Technologies.LAMBDA.value:
            pass

    @classmethod
    def merge(cls):
        return cls._data_process.merge()
