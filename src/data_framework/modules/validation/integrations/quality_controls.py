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
from data_framework.modules.config.model.flows import Technologies, DatabaseTable
from data_framework.modules.storage.interface_storage import Layer
from data_framework.modules.utils.debug import debug_code
from data_framework.modules.exception.validation_exceptions import (
    QualityControlsError,
    FailedRulesError,
    ValidationFunctionNotFoundError,
    ParentNotConfiguredError,
    RuleComputeError
)
from typing import Any, Dict
import pandas as pd


class QualityControls(InterfaceQualityControls):

    def __init__(self):
        self.config = config()
        self.logger = logger
        self.data_process = CoreDataProcess()
        self.technology = self.config.current_process_config().processing_specifications.technology
        self.parent = None
        self.master_table = ControlsTable.master()
        self.dataset_table = ControlsTable.dataset()
        self.results_table = ControlsTable.results()

    def set_parent(self, parent: Any) -> None:
        self.parent = parent

    def validate(
        self,
        layer: Layer,
        table_config: DatabaseTable,
        df_data: Any = None,
        **kwargs: Dict[str, Any]
    ) -> ControlsResponse:
        try:
            self.logger.info(f'Validating table {table_config.full_name}')
            df_rules = self._get_active_rules(layer, table_config)
            if self.data_process.is_empty(df_rules):
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
                df_results = self._compute_rules(df_data, df_rules, **kwargs)
                if df_results is not None:
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
            response = ControlsResponse(
                success=False,
                error=e,
                table=table_config,
                data=df_data,
                rules=None,
                results=None,
                overall_result=False
            )
            raise QualityControlsError(table_name=table_config.full_name)
        return response

    def _get_active_rules(self, layer: Layer, table_config: DatabaseTable) -> Any:
        response_rules = self.data_process.read_table(
            self.dataset_table.table_config.database_relation,
            self.dataset_table.table_config.table,
            self.dataset_table.filter_rules(layer, table_config),
            columns=self.dataset_table.mandatory_columns
        )
        response_master = self.data_process.read_table(
            self.master_table.table_config.database_relation,
            self.master_table.table_config.table,
            columns=self.master_table.mandatory_columns
        )
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
        df_rules = self.data_process.overwrite_columns(
            response.data, common_columns,
            custom_column_suffix='_rules',
            default_column_suffix='_master'
        ).data
        self.logger.info(
            f'Obtained {self.data_process.count_rows(df_rules)} rules ' +
            f'for layer {layer.value} and table {table_config.full_name}'
        )
        return df_rules

    def _get_overall_result(self, df_rules: Any, df_results: Any) -> bool:
        response = self.data_process.join(
            self.data_process.select_columns(
                df_rules,
                ['control_master_id', 'control_table_id', 'blocking_control_indicator', 'control_threshold_rag_max']
            ).data,
            self.data_process.select_columns(
                df_results,
                ['control_master_id', 'control_table_id', 'control_metric_value']
            ).data,
            how='inner',
            left_on=['control_master_id', 'control_table_id'],
        )
        # Check if there are blocker controls with KO result
        df = response.data
        if self.technology == Technologies.EMR:
            from pyspark.sql.functions import col, when
            # PySpark
            failed_controls = df.withColumn(
                'control_threshold_rag_max',
                when(col('control_threshold_rag_max').isNull(), 1.0).otherwise(col('control_threshold_rag_max'))
            ).filter(
                (col('blocking_control_indicator')) & (col('control_metric_value') < col('control_threshold_rag_max'))
            )
        else:
            # Pandas
            df['control_threshold_rag_max'] = df['control_threshold_rag_max'].fillna(1.0)
            failed_controls = df[
                (df['blocking_control_indicator']) &
                (df['control_metric_value'] < df['blocking_control_indicator'])
            ]
        if self.data_process.is_empty(failed_controls):
            return True
        else:
            return False

    def _insert_results(self, df_results: Any) -> None:
        self.data_process.insert_dataframe(df_results, self.results_table.table_config)
        self.logger.info(
            f'Successfully written control results in table {self.results_table.table_config.full_name}'
        )

    def _compute_rules(self, df_data: Any, df_rules: Any, **kwargs: Dict[str, Any]) -> Any:
        if debug_code:
            self.logger.info('Controls definition:')
            self.data_process.show_dataframe(df_rules)
        if self.technology == Technologies.EMR:
            # Transform into Pandas dataframe
            df_rules = df_rules.toPandas()
        # Compute all rules
        df_results = df_rules.apply(self._compute_generic_rule, axis=1, args=(df_data,), **kwargs)
        if not df_results.empty:
            # Remove rules that raised an exception
            df_results = df_results[
                (df_results['control_master_id'].notna()) & (df_results['control_table_id'].notna())
            ]
        if len(df_rules) != len(df_results):
            raise FailedRulesError(n_failed_rules=(len(df_rules)-len(df_results)))
        if not df_results.empty:
            if self.technology == Technologies.EMR:
                # Transform into PySpark dataframe
                response = self.data_process.create_dataframe(df_results)
                df_results = response.data
            if debug_code:
                self.logger.info('Controls results:')
                self.data_process.show_dataframe(df_results)
            return df_results

    def _compute_generic_rule(self, rule_definition: pd.Series, df_data: Any, **kwargs: Dict[str, Any]) -> pd.Series:
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
                self._compute_sql_rule(rule, **kwargs)
            elif rule.algorithm.algorithm_type == AlgorithmType.PYTHON.value:
                self._compute_python_rule(rule, df_data, **kwargs)
            elif rule.algorithm.algorithm_type == AlgorithmType.REGEX.value:
                self._compute_regex_rule(rule, df_data)
            rule_result = rule.result.to_series()
            self.logger.info(f'Successfully computed rule {rule.id}')
            return rule_result
        except Exception:
            error = RuleComputeError(rule_id=rule.id, rule_type=rule.algorithm.algorithm_type)
            self.logger.error(error.format_exception())
            return pd.Series()

    def _compute_sql_rule(self, rule: ControlRule, **kwargs: Dict[str, Any]) -> None:
        query = rule.algorithm.algorithm_description.format(
            # TODO: usar kwargs
            file_date=self.config.parameters.file_date,
            file_name=self.config.parameters.file_name,
        )
        self.logger.info(f'Executing query {query}')
        # TODO: aplicar query sobre dataframe de datos en vez de BBDD
        response = self.data_process.query(query)
        result = rule.calculate_result(response.data)
        if result.invalid_identifiers:
            rule.result.add_detail(f"Invalid records: {', '.join(result.invalid_identifiers)}")

    def _compute_python_rule(self, rule: ControlRule, df_data: Any, **kwargs: Dict[str, Any]) -> None:
        if not self.parent:
            raise ParentNotConfiguredError()
        try:
            function_name = rule.algorithm.algorithm_description
            validation_function = getattr(self.parent, function_name)
            if rule.level == ControlLevel.DATA.value:
                validation_function(rule, df_data, **kwargs)
            elif rule.level == ControlLevel.FILE.value:
                validation_function(rule, **kwargs)
        except AttributeError:
            raise ValidationFunctionNotFoundError(function_name=function_name)

    def _compute_regex_rule(self, rule: ControlRule, df_data: Any) -> None:
        pattern = rule.algorithm.algorithm_description
        columns = rule.field_list
        target_columns = ['identifier', 'value']
        response = self.data_process.stack_columns(df_data, columns, target_columns)
        df_result = response.data
        if self.technology == Technologies.EMR:
            from pyspark.sql.functions import col, when, lit
            df_result = df_result \
                .withColumn('value', when(col('value').isNull(), lit('')).otherwise(col('value'))) \
                .withColumn('result', col('value').rlike(pattern))
        else:
            df_result['value'] = df_result['value'].fillna('')
            df_result['result'] = df_result['value'].str.match(pattern)
        df_result = self.data_process.select_columns(df_result, ['identifier', 'result']).data
        result = rule.calculate_result(df_result)
        if result.valid_identifiers:
            rule.result.add_detail(f"Valid columns: {', '.join(result.unique_valid_identifiers)}")
        if result.invalid_identifiers:
            rule.result.add_detail(f"Invalid columns: {', '.join(result.unique_invalid_identifiers)}")
