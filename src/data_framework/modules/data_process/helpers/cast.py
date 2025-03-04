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

    def decode_cast(self, column_name, column_type):
        query = f'{column_name}'
        if column_type in ('int', 'double', 'float', 'date', 'timestamp') or column_type.startswith('decimal'):
            query = f'TRY_CAST({column_name} AS {column_type.upper()}) AS {column_name}'
        elif column_type == 'boolean':
            l_true = "'true', 't', 'yes', 'y', 'si', 's', '1'"
            l_false = "'false', 'f', 'no', 'n', '0'"
            query = f"(CASE WHEN LOWER({column_name}) in ({l_true}) THEN true " \
                f'WHEN LOWER({column_name}) IN ({l_false}) THEN false ELSE null END) AS {column_name}'
        
        return query

    def build_query_datacast(self, columns, types, tabla, where=None):
        cols_types = dict(zip(columns, types))
        sql_decode = []

        for column, type in cols_types.items():
            fix_column_name = f"`{column}`"

            column_decode = self.decode_cast(column_name=fix_column_name, column_type=type)
            sql_decode.append(column_decode)

        
        sql_final = f"SELECT {', '.join(sql_decode)} FROM {tabla}"
        if where:
            sql_final = f"{sql_final} WHERE {where}"

        return sql_final

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
