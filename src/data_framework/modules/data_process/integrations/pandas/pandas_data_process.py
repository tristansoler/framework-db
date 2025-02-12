from data_framework.modules.data_process.interface_data_process import (
    DataProcessInterface,
    ReadResponse,
    WriteResponse,
)
from data_framework.modules.data_process.helpers.athena import AthenaClient
from data_framework.modules.data_process.helpers.cast import Cast
from data_framework.modules.utils.logger import logger
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.exception.data_process_exceptions import (
    ReadDataError,
    WriteDataError,
    DataProcessError
)
from typing import List, Any
import pandas as pd
from pandas import DataFrame


class PandasDataProcess(DataProcessInterface):

    def __init__(self):
        self.athena = AthenaClient()

    def merge(self, dataframe: DataFrame, table_config: DatabaseTable, custom_strategy: str = None) -> WriteResponse:
        raise NotImplementedError('Function merge not available in Pandas data process')

    def datacast(
        self,
        table_source: DatabaseTable,
        table_target: DatabaseTable
    ) -> ReadResponse:
        raise NotImplementedError('Function datacast not available in Pandas data process')

    def read_table(self, database: str, table: str, filter: str = None, columns: List[str] = None) -> ReadResponse:
        # TODO: use DatabaseTable instead of database and table strings
        try:
            if columns:
                columns_str = ', '.join(columns)
                query = f"SELECT {columns_str} FROM {database}.{table}"
            else:
                query = f"SELECT * FROM {database}.{table}"
            if filter:
                query += f" WHERE {filter}"
            df = self.athena.execute_query(query)
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise ReadDataError(query=query)

    def delete_from_table(self, table_config: DatabaseTable, _filter: str) -> WriteResponse:
        raise NotImplementedError('Function delete_from_table not available in Pandas data process')

    def insert_dataframe(self, dataframe: DataFrame, table_config: DatabaseTable) -> WriteResponse:
        try:
            query = Cast().get_query_to_insert_dataframe(dataframe, table_config)
            self.athena.execute_query(query, read_output=False)
            return WriteResponse(success=True, error=None)
        except Exception:
            raise WriteDataError(database=table_config.database_relation, table=table_config.table)

    def join(
        self,
        df_1: DataFrame,
        df_2: DataFrame,
        how: str,
        left_on: List[str],
        right_on: List[str] = None,
        left_suffix: str = '_df_1',
        right_suffix: str = '_df_2'
    ) -> ReadResponse:
        try:
            if how not in ['inner', 'left', 'right', 'outer']:
                raise ValueError(
                    f'Invalid parameter value: how={how}. Allowed values: inner, left, right, outer'
                )
            if not right_on:
                right_on = left_on
            if len(left_on) != len(right_on):
                raise ValueError(
                    'Number of columns in left_on and right_on parameters must be the same. ' +
                    f'left_on: {len(left_on)} columns. right_on: {len(right_on)} columns'
                )
            else:
                # Perform join
                df_result = df_1.merge(
                    df_2,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffixes=(left_suffix, right_suffix)
                )
            return ReadResponse(success=True, error=None, data=df_result)
        except Exception:
            raise DataProcessError(error_message='Error performing join of two dataframes')

    def create_dataframe(self, data: Any, schema: str = None) -> ReadResponse:
        try:
            if schema:
                # TODO: implementar
                raise NotImplementedError(
                    'Function create_dataframe with custom schema feature not available in Pandas data process'
                )
            else:
                df = pd.DataFrame(data)
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise DataProcessError(error_message='Error creating dataframe')

    def query(self, sql: str) -> ReadResponse:
        try:
            df = self.athena.execute_query(sql)
            return ReadResponse(success=True, error=None, data=df)
        except Exception:
            raise ReadDataError(query=sql)

    def overwrite_columns(
        self,
        dataframe: DataFrame,
        columns: List[str],
        custom_column_suffix: str,
        default_column_suffix: str,
        drop_columns: bool = True
    ) -> ReadResponse:
        try:
            for column in columns:
                custom_column = column + custom_column_suffix
                default_column = column + default_column_suffix
                dataframe[column] = dataframe[custom_column].fillna(dataframe[default_column])
                if drop_columns:
                    dataframe = dataframe.drop([custom_column, default_column], axis=1)
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError()

    def unfold_string_values(self, dataframe: DataFrame, column_name: str, separator: str) -> ReadResponse:
        raise NotImplementedError('Function unfold_string_values not available in Pandas data process')

    def add_dynamic_column(
        self,
        dataframe: DataFrame,
        new_column: str,
        reference_column: str,
        available_columns: List[str],
        default_value: Any = None
    ) -> ReadResponse:
        raise NotImplementedError('Function add_dynamic_column not available in Pandas data process')

    def stack_columns(
        self,
        dataframe: DataFrame,
        source_columns: List[str],
        target_columns: List[str]
    ) -> ReadResponse:
        try:
            if len(target_columns) != 2:
                raise ValueError(f'Must specify two columns as target_columns. Found {target_columns}')
            dataframe = dataframe.reset_index(names='index')
            dataframe = pd.melt(
                dataframe,
                id_vars=['index'],
                value_vars=source_columns,
                var_name=target_columns[0],
                value_name=target_columns[1]
            )
            dataframe = dataframe[target_columns]
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError()

    def is_empty(self, dataframe: DataFrame) -> bool:
        if dataframe is not None:
            return dataframe.empty
        else:
            return True

    def count_rows(self, dataframe: DataFrame) -> int:
        return len(dataframe)

    def select_columns(self, dataframe: DataFrame, columns: List[str]) -> ReadResponse:
        try:
            dataframe = dataframe[columns]
            return ReadResponse(success=True, error=None, data=dataframe)
        except Exception:
            raise DataProcessError('Error selecting columns of a dataframe')

    def show_dataframe(self, dataframe: DataFrame) -> WriteResponse:
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        logger.info(dataframe)
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
