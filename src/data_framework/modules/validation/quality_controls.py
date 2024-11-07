from data_framework.modules.config.core import config, Config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from typing import Any, List


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
        # TODO: parametrizar
        self.controls_database = 'rl_funds_common'
        self.master_table = 'controls_master'
        self.dataset_table = 'controls_dataset'
        self.results_table = 'controls_results'

    def validate(self, df: Any, database: str, table: str) -> bool:
        try:
            df_rules = self.get_active_quality_rules(database, table)
            results = self.compute_rules(df, df_rules)
            self.insert_rule_results(results)
        except Exception as e:
            self.logger.error(f'Error validating data from {database}.{table}: {e}')
            return False

    def get_active_quality_rules(self, database: str, table: str) -> Any:
        _filter = f"""
            control_database = '{database}' AND
            control_table = '{table}' AND
            active_control_indicator
        """
        response_rules = self.data_process.read_table(
            self.controls_database, self.dataset_table, _filter
        )
        if response_rules.success:
            response_master = self.data_process.read_table(
                self.controls_database, self.master_table
            )
            if response_master.success:
                df_rules = response_rules.data
                df_master = response_master.data
                df_rules = self.data_process.join(
                    df_rules, df_master, on=['control_master_id'], how='left'
                )
                # TODO: sobrescribir umbrales de controls_master con los de controls_dataset
                return df_rules
            else:
                raise response_master.error
        else:
            raise response_rules.error

    def compute_rules(self, df: Any, df_rules: Any) -> List[dict]:
        results = []
        # TODO
        return results

    def insert_rule_results(self, results: List[dict]) -> None:
        # TODO: parametrizar
        schema = {
            'Initial_Date': {'type': 'date', 'is_null': False},
            'End_Date': {'type': 'date', 'is_null': False},
            'Control_Table_Id': {'type': 'string', 'is_null': False},
            'Control_Outcome': {'type': 'string', 'is_null': False},
            'Control_Detail': {'type': 'string', 'is_null': False},
            'data_date': {'type': 'date', 'is_null': False},
            'control_master_id': {'type': 'string', 'is_null': True},
            'Control_Result': {'type': 'string', 'is_null': False},
        }
        response = self.data_process.create_dataframe(schema, results)
        if response.success:
            df_results = response.data

        else:
            raise response.error
