from data_framework.modules.validation.interface_quality_controls import (
    InterfaceQualityControls,
    ControlsResponse,
    ControlsTable,
    ControlRule,
    AlgorithmType,
    ControlLevel
)
from data_framework.modules.config.core import config
from data_framework.modules.utils.logger import logger
from data_framework.modules.data_process.core_data_process import CoreDataProcess
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.utils.debug import debug_code
from typing import Any
from traceback import format_exc
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import DataFrame


class QualityControls(InterfaceQualityControls):

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.data_process = CoreDataProcess()
        self.parent = None
        self.master_table = ControlsTable.master()
        self.dataset_table = ControlsTable.dataset()
        self.results_table = ControlsTable.results()

    def set_parent(self, parent: Any) -> None:
        self.parent = parent

    def validate(self, layer: Layer, table_config: DatabaseTable, df_data: DataFrame = None) -> ControlsResponse:
        try:
            self.logger.info(f'Validating table {table_config.full_name}')
            df_rules = self._get_active_rules(layer, table_config)
            if df_rules.isEmpty():
                self.logger.info(f'No rules defined for table {table_config.full_name}')
                response = ControlsResponse(
                    success=True,
                    error=None,
                    table=table_config,
                    data=df_data,
                    rules=None,
                    results=None,
                    overall_result=True
                )
            else:
                df_results = self._compute_rules(df_data, df_rules)
                if df_results:
                    self._insert_results(df_results)
                    overall_result = self._get_overall_result(df_rules, df_results)
                    response = ControlsResponse(
                        success=True,
                        error=None,
                        table=table_config,
                        data=df_data,
                        rules=df_rules,
                        results=df_results,
                        overall_result=overall_result
                    )
                else:
                    self.logger.info('No control results to persist')
                    response = ControlsResponse(
                        success=True,
                        error=None,
                        table=table_config,
                        data=df_data,
                        rules=df_rules,
                        results=None,
                        overall_result=True
                    )
        except Exception as e:
            self.logger.error(
                f'Error validating data from {table_config.full_name}\n' +
                f'Exception:\n   {type(e).__name__}\nError:\n    {e}\nTrace:\n  {format_exc()}'
            )
            response = ControlsResponse(
                success=False,
                error=e,
                table=table_config,
                data=df_data,
                rules=None,
                results=None,
                overall_result=False
            )
        return response

    def _get_active_rules(self, layer: Layer, table_config: DatabaseTable) -> DataFrame:
        response_rules = self.data_process.read_table(
            self.dataset_table.table_config.database_relation,
            self.dataset_table.table_config.table,
            self.dataset_table.filter_rules(layer, table_config),
            columns=self.dataset_table.mandatory_columns
        )
        if response_rules.success:
            response_master = self.data_process.read_table(
                self.master_table.table_config.database_relation,
                self.master_table.table_config.table,
                columns=self.master_table.mandatory_columns
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
                    self.logger.info(
                        f'Obtained {df_rules.count()} rules for layer {layer.value} and table {table_config.full_name}'
                    )
                    return df_rules
                else:
                    raise response.error
            else:
                raise response_master.error
        else:
            raise response_rules.error

    def _get_overall_result(self, df_rules: DataFrame, df_results: DataFrame) -> bool:
        response = self.data_process.join(
            df_rules.select(*['control_master_id', 'control_table_id', 'blocking_control_indicator']),
            df_results.select(*['control_master_id', 'control_table_id', 'control_result']),
            how='inner',
            left_on=['control_master_id', 'control_table_id'],
        )
        if not response.success:
            raise response.error
        # Check if there are blocker controls with KO result
        failed_controls = response.data.filter((col('blocking_control_indicator')) & ~(col('control_result')))
        if failed_controls.isEmpty():
            return True
        else:
            return False

    def _insert_results(self, df_results: DataFrame) -> None:
        response = self.data_process.merge(df_results, self.results_table.table_config)
        if response.success:
            self.logger.info(
                f'Successfully written control results in table {self.results_table.table_config.full_name}'
            )
        else:
            raise response.error

    def _compute_rules(self, df_data: DataFrame, df_rules: DataFrame) -> DataFrame:
        if debug_code:
            self.logger.info('Controls definition:')
            df_rules.show(truncate=False)
        # Transform into Pandas dataframe
        pdf_rules = df_rules.toPandas()
        # Compute all rules
        pdf_results = pdf_rules.apply(self._compute_generic_rule, axis=1, args=(df_data,))
        if len(pdf_rules) != len(pdf_results):
            raise ValueError('Some rules could not be executed correctly')
        # Transform into PySpark dataframe
        if pdf_results is not None and not pdf_results.empty:
            response = self.data_process.create_dataframe(pdf_results)
            if response.success:
                df_results = response.data
                if debug_code:
                    self.logger.info('Controls results:')
                    df_results.show(truncate=False)
                return df_results

    def _compute_generic_rule(self, rule_definition: pd.Series, df_data: DataFrame) -> pd.Series:
        try:
            # Parse rule
            rule = ControlRule.from_series(rule_definition)
            rule.result.set_data_date()
            if debug_code:
                self.logger.info(f'Parsed rule: {rule}')
            # Validate rule
            rule.validate()
            self.logger.info(f'Rule {rule.id} is valid')
            # Calculate rule
            self.logger.info(f'Computing rule {rule.id}')
            if rule.algorithm.algorithm_type == AlgorithmType.SQL.value:
                self._compute_sql_rule(rule)
            elif rule.algorithm.algorithm_type == AlgorithmType.PYTHON.value:
                self._compute_python_rule(rule, df_data)
            rule_result = rule.result.to_series()
            self.logger.info(f'Successfully computed rule {rule.id}')
            return rule_result
        except Exception as e:
            self.logger.error(f'Error computing rule {rule.id}: {e}')

    def _compute_sql_rule(self, rule: ControlRule) -> None:
        query = rule.algorithm.algorithm_description.format(
            # TODO: estos parámetros son custom para cada regla
            file_date=self.config.parameters.file_date,
            country=self.file_country
        )
        self.logger.info(f'Executing query {query}')
        response = self.data_process.query(query)
        if response.success:
            results = response.data.select(response.data.columns[0]).rdd.flatMap(lambda x: x).collect()
            # TODO: la query debería proporcionar el resto de info a reflejar en el resultado
            rule.calculate_result(results)
        else:
            raise response.error

    def _compute_python_rule(self, rule: ControlRule, df_data: DataFrame) -> None:
        # TODO: ver si realmente es necesario pasar el dataframe con los datos
        # TODO: ¿qué sucede si una función necesita más argumentos adicionales?
        try:
            if not self.parent:
                self.logger.error('QualityControls parent is not set. Configure it using set_parent method')
            function_name = rule.algorithm.algorithm_description
            validation_function = getattr(self.parent, function_name)
            if rule.level == ControlLevel.DATA.value:
                validation_function(rule, df_data)
            elif rule.level == ControlLevel.FILE.value:
                validation_function(rule)
        except AttributeError as e:
            raise ValueError(f'Error executing Python function {function_name}: {e}')
