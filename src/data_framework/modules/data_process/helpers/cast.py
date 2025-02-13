"""
"""

from data_framework.modules.utils.logger import logger
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.exception.data_process_exceptions import CastQueryError
from functools import reduce
from typing import Any, Dict
import pandas as pd
from pandas import DataFrame, Series


class Cast:

    def __init__(self):
        self.logger = logger
        self.catalogue = CoreCatalogue()

    def decode_cast(self, col, coltype):
        q = f'{col}'
        if coltype in ('int', 'double', 'float', 'date', 'timestamp') or coltype.startswith('decimal'):
            q = f"try_cast({col} AS {coltype.upper()}) as {col}, " \
                f"case when try_cast({col} AS {coltype.upper()}) is null " \
                f"then '{col}: valor {coltype} inválido' end as {col}_control_cast"
        elif coltype == 'boolean':
            l_true = "'true', 't', 'yes', 'y', 'si', 's', '1'"
            l_false = "'false', 'f', 'no', 'n', '0'"
            q = f"(case when lower({col}) in ({l_true}) then true " \
                f"when lower({col}) in ({l_false}) then false else null end) as {col}, " \
                f"(case when lower({col}) not in ({l_true}, {l_false}) " \
                f"then '{col}: valor {coltype} inválido' end) as {col}_control_cast"
        return q

    def build_query_datacast(self, l_columns, l_types, tabla, where=None):
        d_cols_types = dict(zip(l_columns, l_types))
        l_columns_no_string = [f"{key}_control_cast" for key, val in d_cols_types.items() if val != 'string']

        l_decode_cols = [self.decode_cast(key, val) for key, val in d_cols_types.items()]
        decode_cols = reduce(lambda a, b: a + ', ' + b, l_decode_cols)
        subquery = f"select {decode_cols} from {tabla}"
        if where:
            subquery = f"{subquery} where {where}"

        columns_query = reduce(lambda a, b: a + ', ' + b, l_columns)
        columns_query_control = ", ' | ', ".join(l_columns_no_string)
        columns_query = f"{columns_query}, concat({columns_query_control}) as control_cast"

        query = f"select {columns_query} from ({subquery})"

        return query

    def get_query_datacast(
        self,
        table_source: DatabaseTable,
        table_target: DatabaseTable
    ) -> Any:
        try:
            partitioned = True

            schema_source = self.catalogue.get_schema(table_source.database_relation, table_source.table)
            l_cols_source = schema_source.schema.get_column_names(partitioned)

            schema_target = self.catalogue.get_schema(table_target.database_relation, table_target.table)
            l_types_target = schema_target.schema.get_type_columns(partitioned)

            query = self.build_query_datacast(
                l_cols_source,
                l_types_target,
                "data_to_cast",
                table_source.sql_where
            )

            return query
        except Exception:
            raise CastQueryError()

    def get_query_to_insert_dataframe(self, dataframe: DataFrame, table_config: DatabaseTable) -> str:
        try:
            # Obtain data types of the target table
            response = self.catalogue.get_schema(table_config.database_relation, table_config.table)
            target_schema = response.schema
            target_columns = target_schema.get_column_names(partitioned=True)
            target_types = target_schema.get_type_columns(partitioned=True)
            column_types = {column: _type for column, _type in zip(target_columns, target_types)}
            # Select dataframe needed columns
            dataframe = dataframe[target_columns]
            # Cast dataframe values
            rows_list = dataframe.apply(self.cast_df_row, axis=1, args=(column_types,))
            rows = ', '.join(rows_list)
            # Build INSERT query
            columns = ', '.join(dataframe.columns)
            query = f"""
                INSERT INTO {table_config.database_relation}.{table_config.table}
                ({columns}) VALUES {rows};
            """
            return query
        except Exception:
            raise CastQueryError()

    def cast_df_row(self, row: Series, column_types: Dict[str, str]) -> str:
        values = ', '.join(
            self.cast_df_value(value, column_types[column])
            for column, value in row.items()
        )
        return f'({values})'

    def cast_df_value(self, value: Any, _type: str) -> str:
        if pd.isna(value):
            return 'NULL'
        elif _type == 'string':
            return f"'{value}'"
        elif _type == 'timestamp':
            return f"timestamp '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        elif _type == 'date':
            return f"date('{value.strftime('%Y-%m-%d')}')"
        else:
            return str(value)
