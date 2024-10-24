"""
Clase para el casteo de las columnas de un dataframe.
Además del casteo se añade columna para devolver los posibles problemas de casteo

# Datatypes a convertir:
# string - ok
# int - ok
# double - ok
# date - ok
# boolean - ok
# datetime TODO
# timestamp TODO

# TODO: Está hecho para query en Athena. Cuando funcione spark hay que cambiar las funciones de casteo.
# Todo: ¿cómo se van a pasar los datos de la bd y de las tablas? Quizás no hace falta pasar la bd y tabla origen, 
# si tenemos un df origen tenemos las columnas origen.
"""

from data_framework.modules.utils.logger import logger
from data_framework.modules.config.core import config
from data_framework.modules.catalogue.core_catalogue import CoreCatalogue
from functools import reduce


class Cast():

    def __init__(self):

        self.logger = logger
        self.config = config()

    def decode_cast(self, col, coltype):
        q =  f'{col}'
        if coltype in ('int', 'double', 'date'):
            q = f"try_cast({col} AS {coltype.upper()}) as {col}, "\
                f"case when try_cast({col} AS {coltype.upper()}) is null then '{col}: valor {coltype} inválido' end as {col}_control_cast"
        elif coltype == 'boolean':
            l_true = "'true', 't', 'yes', 'y', 'si', 's', '1'"
            l_false = "'false', 'f', 'no', 'n', '0'"
            q = f"(case when lower({col}) in ({l_true}) then true when lower({col}) in ({l_false}) then false else null end) as {col}, "\
                f"(case when lower({col}) not in ({l_true}, {l_false})  then '{col}: valor {coltype} inválido' end) as {col}_control_cast"
        return q


    def build_query_datacast(self, l_columns, l_types, tabla, where=None):
        d_cols_types = dict(zip(l_columns, l_types))
        l_columns_no_string =  [f"{key}_control_cast" for key, val in d_cols_types.items() if val != 'string']

        l_decode_cols = [self.decode_cast(key, val) for key, val in d_cols_types.items()]
        decode_cols = reduce(lambda a, b: a + ', ' + b, l_decode_cols)
        subquery = f"select {decode_cols} from {tabla}"
        if where:
            subquery = f"{subquery} where {where}"

        columns_query =  reduce(lambda a, b: a + ', ' + b, l_columns)
        columns_query_control = ", ' | ', ".join(l_columns_no_string)
        columns_query = f"{columns_query}, concat({columns_query_control}) as control_cast"

        query = f"select {columns_query} from ({subquery})"

        return query

    
    def get_query_datacast_simple(self, l_cols_source, l_types_target, table, where_source):
        query = self.build_query_datacast(l_cols_source, l_types_target, table, where_source)
        return query


    def get_query_datacast(self, database_source, table_source, database_target, table_target):

        catalogue = CoreCatalogue._catalogue
        schema_source = catalogue.get_schema(database_source, table_source)
        l_cols_source = schema_source.schema.get_column_names()

        schema_target = catalogue.get_schema(database_target, table_target)
        l_types_target = schema_target.schema.get_type_columns()

        where_source = f"datadate = '{self.config.parameters.file_date}'"

        query = self.get_query_datacast_simple(l_cols_source, l_types_target, f"{database_source}.{table_source}", where_source)

        return query
