from data_framework.modules.dataflow.interface_dataflow import DataFlowInterface


class RawToStaging(DataFlowInterface):

    def __init__(self):
        super().__init__()
