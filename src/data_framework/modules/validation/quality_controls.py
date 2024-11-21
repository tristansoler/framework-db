from data_framework.modules.config.core import config, Config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.utils.debug import debug_code
from typing import Any, Union
from datetime import datetime
from pandas import DataFrame, Series


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
        self.parent = None
        # TODO: parametrizar tablas
        self.master_table = DatabaseTable(
            database='funds_common',
            table='controls_master_v2',
            primary_keys=['control_master_id']
        )
        self.dataset_table = DatabaseTable(
            database='funds_common',
            table='controls_dataset_v2',
            primary_keys=['control_master_id', 'control_table_id']
        )
        self.results_table = DatabaseTable(
            database='funds_common',
            table='controls_results_v2',
            primary_keys=[
                'control_master_id', 'control_table_id',
                'initial_date', 'end_date'
            ]
        )

    def set_parent(self, parent) -> None:
        self.parent = parent

    def validate(self, df_data: Any, layer: Layer, table_config: DatabaseTable) -> dict:
        try:
            response = {
                'success': True,
                'error': None,
                'table': table_config,
                'data': df_data,
                'rules': None,
                'results': None
            }
            self.logger.info(f'Validating table {table_config.full_name}')
            df_rules = self.get_active_rules(layer, table_config)
            if df_rules.isEmpty():
                self.logger.info(f'No rules defined for table {table_config.full_name}')
            else:
                df_data = self.filter_input_dataframe(df_data, table_config, df_rules)
                df_results = self.compute_rules(df_data, df_rules)
                self.insert_results(df_results)
                response['data'] = df_data
                response['rules'] = df_rules
                response['results'] = df_results
        except Exception as e:
            self.logger.error(f'Error validating data from {table_config.full_name}: {e}')
            response['success'] = False
            response['error'] = e
        return response

    def get_active_rules(self, layer: Layer, table_config: DatabaseTable) -> Any:
        _filter = f"""
            control_database = '{table_config.database}' AND
            control_table = '{table_config.table}' AND
            control_layer = '{layer.value}' AND
            active_control_indicator
        """
        response_rules = self.data_process.read_table(
            self.dataset_table.database_relation, self.dataset_table.table, _filter,
            columns=[
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
        if response_rules.success:
            response_master = self.data_process.read_table(
                self.master_table.database_relation, self.master_table.table,
                columns=[
                    "control_master_id",
                    "control_level",
                    "control_threshold_min",
                    "control_threshold_max",
                    "control_threshold_type"
                ]
            )
            if response_master.success:
                df_rules = response_rules.data
                df_master = response_master.data
                common_columns = list(set(df_rules.columns) & set(df_master.columns) - set(['control_master_id']))
                response = self.data_process.join(
                    df_rules, df_master,
                    how='left',
                    left_on=['control_master_id'],
                    left_suffix='_rules',
                    right_suffix='_master'
                )
                if response.success:
                    df_rules = self.data_process.overwrite_columns(
                        response.data, common_columns,
                        custom_column_suffix='_rules',
                        default_column_suffix='_master'
                    ).data
                    self.logger.info(f'Obtained {df_rules.count()} rules for table {table_config.full_name}')
                    return df_rules
                else:
                    raise response.error
            else:
                raise response_master.error
        else:
            raise response_rules.error

    def insert_results(self, df_results: Any) -> None:
        if df_results:
            response = self.data_process.insert_dataframe(df_results, self.results_table)
            if response.success:
                self.logger.info(f'Successfully written control results in table {self.results_table.full_name}')
            else:
                raise response.error
        else:
            self.logger.info('No control results to persist')

    def filter_input_dataframe(self, df_data: Any, table_config: DatabaseTable, df_rules: Any) -> Any:
        control_fields = self.data_process.unfold_string_values(df_rules, 'control_field', ',').data
        primary_keys = table_config.primary_keys
        columns_to_keep = list(set(control_fields) | set(primary_keys))
        return df_data.select(*columns_to_keep)

    def compute_rules(self, df_data: Any, df_rules: Any) -> Any:
        if debug_code:
            self.logger.info('Controls definition:')
            df_rules.printSchema()
            df_rules.show(truncate=False)
        # Transform into Pandas dataframe
        pdf_rules = df_rules.toPandas()
        # Compute all rules
        pdf_results = pdf_rules.apply(self.compute_generic_rule, axis=1, args=(df_data,))
        if not pdf_results.empty:
            df_results = self.data_process.create_dataframe(pdf_results).data
            if debug_code:
                self.logger.info('Controls results:')
                df_results.printSchema()
                df_results.show(truncate=False)
            return df_results

    def compute_generic_rule(self, rule: Series, df_data: Any) -> Series:
        try:
            rule_id = rule['control_table_id']
            self.logger.info(f'Computing rule {rule_id}')
            # Standard result
            rule_result = {
                'control_table_id': rule_id,
                'control_master_id': rule['control_master_id'],
                'data_date': datetime.strptime(self.config.parameters.file_date, '%Y-%m-%d'),
                'initial_date': datetime.now(),
                'end_date': None,
                'control_result': None,
                'control_outcome': {'total': None, 'value': None},
                'control_metric_value': None,
                'control_detail': None
            }
            algorithm_type = rule['control_algorithm_type']
            if algorithm_type == 'SQL':
                self.compute_sql_rule(rule, rule_result)
            elif algorithm_type == 'Python':
                self.compute_python_rule(rule, rule_result, df_data)
            else:
                raise ValueError(f'Rule algorithm type not implemented: {algorithm_type}')
            rule_result['end_date'] = datetime.now()
            rule_result['control_outcome'] = str(rule_result['control_outcome'])
            return Series(rule_result)
        except Exception as e:
            self.logger.error(f'Error computing rule {rule_id}: {e}')

    def compute_sql_rule(self, rule: Series, rule_result: dict) -> None:
        query = rule['control_description'].format(
            # TODO: estos parámetros son custom para cada regla
            file_date=self.config.parameters.file_date,
            country=self.file_country
        )
        self.logger.info(f'Executing query {query}')
        response = self.data_process.query(query)
        if response.success:
            # TODO: la query debería proporcionar el resto de info a reflejar en el resultado
            result_value = self.apply_threshold_type(response.data.toPandas(), rule['control_threshold_type'])
            rule_result['control_result'] = self.apply_threshold_limits(
                result_value,
                rule['control_threshold_min'],
                rule['control_threshold_max']
            )
        else:
            raise response.error

    def compute_python_rule(self, rule: Series, rule_result: dict, df_data: Any) -> None:
        try:
            if not self.parent:
                self.logger.error('QualityControls parent is not set. Configure it using set_parent method')
            function_name = rule['control_description']
            function = getattr(self.parent, function_name)
            function(rule, rule_result, df_data)
        except AttributeError as e:
            raise ValueError(f'Error executing Python function {function_name}: {e}')

    def apply_threshold_type(self, df_result: DataFrame, threshold_type: str) -> float:
        if not threshold_type:
            return 0
        elif threshold_type == 'Count':
            return len(df_result)
        elif threshold_type == 'Percentage' and not df_result.empty:
            return df_result.iat[0, 0] * 100
        elif threshold_type == 'Absolute' and not df_result.empty:
            return abs(df_result.iat[0, 0])
        elif df_result.empty:
            raise ValueError('DataFrame with numeric result is empty')
        else:
            raise ValueError(f'Rule threshold type not implemented: {threshold_type}')

    def apply_threshold_limits(
        self,
        value: float,
        threshold_min: Union[float, None],
        threshold_max: Union[float, None]
    ) -> bool:
        if threshold_min is not None and threshold_max is not None:
            return (threshold_min <= value <= threshold_max)
        elif threshold_min is not None:
            return (threshold_min <= value)
        elif threshold_max is not None:
            return (value <= threshold_max)
        else:
            return True

    def calculate_metric_value(self, rule_result: dict) -> Union[float, None]:
        total_value = rule_result['control_outcome']['total']
        rule_value = rule_result['control_outcome']['value']
        if total_value is not None and rule_value is not None:
            metric_value = rule_value * 100 / total_value if total_value != 0 else 0
            rule_result['control_metric_value'] = metric_value
            return metric_value
