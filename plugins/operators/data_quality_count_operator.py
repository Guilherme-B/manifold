from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityCountOperator(BaseOperator):
    """Asserts the existence of records in the specified table


    Raises
    ------
    ValueError
        the table is not found or contains no records
    """
    COUNT_TEMPLATE_STATEMENT: str = "SELECT count(1) FROM {}"

    @apply_defaults
    def __init__(self, postgres_conn_id: str, table_name: str, *args, **kwargs):

        super(DataQualityCountOperator, self).__init__(*args, **kwargs)

        self._postgres_conn_id = postgres_conn_id
        self._table_name = table_name

    def execute(self, context):
        self.log.info('DataQualityCountOperator::execute starting')

        redshift: PostgresHook = None

        try:
            redshift = PostgresHook(self._postgres_conn_id)
        except:
            self.log.error(
                'DataQualityCountOperator::execute could not connect to Redshift cluster')

        record_count: int = redshift.get_records(DataQualityCountOperator.COUNT_TEMPLATE_STATEMENT.format(self._table_name))

        if len(record_count) < 1 or len(record_count[0]) < 1:
            raise ValueError(f'DataQualityCountOperator::execute validation failed, table {self._table_name} not found')

        record_count = record_count[0][0]

        if record_count < 1:
            raise ValueError(f'DataQualityCountOperator::execute validation failed, table {self._table_name} contains no records')

        print(f'DataQualityCountOperator::execute validation passed for table {self._table_name}')

        

