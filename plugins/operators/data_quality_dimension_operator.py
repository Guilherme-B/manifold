from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityDimensionOperator(BaseOperator):
    """Asserts the existence of multiple valid records (SCD2)


    Raises
    ------
    ValueError
        there are multiple active records for the selected dimension object
    """
    SCD_VALIDATION_TEMPLATE_STATEMENT: str = '''SELECT count(1) FROM {}
        SELECT 
            count(*)
        FROM 
            {table_name}
        where
            record_end_date = '99990101'
        group by
            {identification_columns}
        having
            count(*) > 1
    '''

    @apply_defaults
    def __init__(self, postgres_conn_id: str, table_name: str, identification_columns: List[str], *args, **kwargs):
        super(DataQualityDimensionOperator, self).__init__(*args, **kwargs)

        self._postgres_conn_id = postgres_conn_id
        self._table_name = table_name
        self._identification_columns = identification_columns

    def execute(self, context):
        self.log.info('DataQualityDimensionOperator::execute starting')

        redshift: PostgresHook = None

        try:
            redshift = PostgresHook(self._postgres_conn_id)
        except:
            self.log.error(
                'DataQualityDimensionOperator::execute could not connect to Redshift cluster')

        # separate the individual columns with a comma to be used in the group by clause
        identification_columns: str = ' ,' .join(self._identification_columns)
        
        record_count: int = redshift.get_records(DataQualityDimensionOperator.SCD_VALIDATION_TEMPLATE_STATEMENT.format(table_name = self._table_name), identification_columns= self.identification_columns)

        if len(record_count) < 1 or len(record_count[0]) < 1:
            raise ValueError(f'DataQualityDimensionOperator::execute validation failed, table {self._table_name} not found')

        record_count = record_count[0][0]

        if record_count > 0:
            raise ValueError(f'DataQualityDimensionOperator::execute validation failed, table {self._table_name} contains multiple active entities')

        print(f'DataQualityDimensionOperator::execute validation passed for table {self._table_name}')

        

