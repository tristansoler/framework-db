from data_framework.modules.config.core import config, Config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from typing import Any


class QualityControls:

    @property
    def config(self) -> Config:
        return self.__config

    @property
    def logger(self):
        return self.__logger

    @property
    def data_process(self) -> CoreDataProcess:
        return self.__data_process

    def __init__(self):
        self.__config = config()
        self.__logger = logger
        self.__data_process = CoreDataProcess()
        self.controls_database = 'rl_funds_common'
        self.master_table = 'controls_master'
        self.dataset_table = 'controls_dataset'
        self.results_table = 'controls_results'

    def validate(self, df: Any, database: str, table: str) -> bool:
        try:
            df_rules = self.get_active_quality_rules(database, table)
            self.compute_rules(df, df_rules)
        except Exception as e:
            self.logger.error(f'Error validating data from {database}.{table}: {e}')
            return False

    def get_active_quality_rules(self, database: str, table: str) -> Any:
        _filter = f"""
            control_database = '{database}' AND
            control_table = '{table}' AND
            active_control_indicator
        """
        response_rules = self.__data_process.read_table_with_filter(
            self.controls_database, self.dataset_table, _filter
        )
        if response_rules.success:
            response_master = self.__data_process.read_table(
                self.controls_database, self.master_table
            )
            if response_master.success:
                df_rules = response_rules.data
                df_master = response_master.data
                df_rules = self.__data_process.join(
                    df_rules, df_master, on=['control_master_id'], how='left'
                )
                return df_rules
            else:
                raise response_master.error
        else:
            raise response_rules.error

    def compute_rules(self, df: Any, df_rules: Any) -> None:
        pass
