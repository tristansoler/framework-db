"""
"""

from data_framework.modules.utils.logger import logger
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from data_framework.modules.config.model.flows import DatabaseTable
from data_framework.modules.exception.data_process_exceptions import CastQueryError
from typing import Any, Dict, List
import pandas as pd
from pandas import DataFrame, Series


class Cast:

    def __init__(self):
        self.logger = logger
        self.catalogue = CoreCatalogue()

    def cast_columns(self, column_name: str, column_type: str) -> str:
        if column_type in ('int', 'double', 'float', 'date', 'timestamp') or column_type.startswith('decimal'):
            query = f'TRY_CAST({column_name} AS {column_type.upper()}) AS {column_name}'
        elif 'struct<' in column_type or 'array<' in column_type:
            query = f"FROM_JSON({column_name}, '{column_type}') AS {column_name}"
        elif column_type == 'boolean':
            query = f"""
                (
                    CASE WHEN LOWER({column_name}) in ('true', 't', 'yes', 'y', 'si', 's', '1') THEN true
                    WHEN LOWER({column_name}) IN ('false', 'f', 'no', 'n', '0') THEN false
                    ELSE null END
                ) AS {column_name}
            """
        else:
            query = f'{column_name}'
        return query

    def build_datacast_query(
        self,
        source_columns: List[str],
        table_target: DatabaseTable,
        view_name: str = 'data_to_cast'
    ) -> str:
        try:
            # Obtain the target schema with the type of each column
            schema_target = self.catalogue.get_schema(table_target.database_relation, table_target.table)
            target_types = schema_target.schema.get_column_type_mapping(partitioned=True)
            # Cast each column to its target type
            casted_columns = [
                self.cast_columns(
                    column_name=f"`{column}`",
                    column_type=target_types.get(column, 'string')
                )
                for column in source_columns
            ]
            # Build SQL query
            query = f"SELECT {', '.join(casted_columns)} FROM {view_name}"
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
