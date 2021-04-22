from re import match

from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DimensionOperator(BaseOperator):

    # SCD part 1: update previous record versions
    _update_statement: str = '''
        update 
            presentation.{target_update}
        set
            record_end_date = current_date - 1
        from
        (
            select
            target.id
            from
            presentation.{target_update} target
            inner join
            staging.{target_base} base
            on
            {match_predicate}
            where
            -- select only the active record to be updated
            target.record_end_date = '99991231'
            and
            target.hash != base.hash
        ) modified_records_
        where
            {target_update}.id = modified_records_.id
            and
            -- select only cases with actual modifications
            modified_records_.id is not null;
    '''

    # SCD part 2: insert new record versions
    _insert_statement: str = '''
        insert into 
            presentation.{target_update} ({insert_columns})
        select
            {base_columns},
            current_date record_start_date,
            '99991231' record_end_date
        from
            staging.{target_base} base
        left join
            presentation.{target_update} target
        on
            {match_predicate}
        where
            -- select only cases with modifications or no record
            (
            -- no previous record
            target.id is null
            or
            -- altered record
            target.hash != base.hash
            );
    '''

    @apply_defaults
    def __init__(self, postgres_conn_id: str, target_table: str, base_table: str, match_columns: List[str], database_name: str = 'dev', *args, **kwargs):

        if postgres_conn_id is None or target_table is None or base_table is None or match_columns is None:
            raise ValueError('DimensionOperator::__init__ missing arguments')

        if len(postgres_conn_id) == 0 or len(target_table) == 0 or len(base_table) == 0 or len(match_columns) == 0:
            raise AttributeError(
                'DimensionOperator::__init__ invalid arguments')

        super(DimensionOperator, self).__init__(*args, **kwargs)

        self._postgres_conn_id = postgres_conn_id
        self._target_table = target_table
        self._base_table = base_table
        self._match_columns = match_columns
        self._database_name = database_name

    def execute(self, context):
        self._hook = PostgresHook(postgres_conn_id=self._postgres_conn_id,
                                  schema=self._database_name)

        query: str = self._generate_upsert_query(
            self._hook, self._target_table, self._base_table, self._match_columns)

        self.log.info('DimensionOperator::execute running query %', query)

        self._hook.run(query, autocommit=True, parameters=None)

        for output in self._hook.conn.notices:
            self.log.info(output)

        self.log.info('DimensionOperator::execute finished running query')

    def _get_insertion_columns(self, hook: PostgresHook, schema_name: str, table_name: str) -> List[str]:
        """Retrieves the list of columns present in the selected table_name within the schema_name schema, in their correct order (table order)

        Parameters
        ----------
        hook : PostgresHook
            The Apache Airflow PostgresHook holding the connection details
        schema_name : str
            The target's schema name
        table_name : str
            The target's table name

        Returns
        -------
        List[str]
            The list of columns present in the table_name within the schema_name schema
        """        
        # Get all column names from the Redshift presentation layer, and exclude ID from the insertion
        base_query = '''
            SELECT 
                column_name 
            FROM 
                information_schema.columns
            WHERE 
                table_schema = '{schema}'
            AND 
                table_name   = '{table}';
        '''

        formatted_query: str = base_query.format(
            schema=schema_name, table=table_name)

        results = hook.get_records(formatted_query)

        columns: List[str] = [column[0]
                              for column in results if column[0] != 'id']

        return columns

    def _generate_upsert_query(self, hook: PostgresHook, target_table: str, base_table: str, match_columns: List[str]) -> str:
        """Generates an AWS Redshift SQL upsert (update and insert) statement using match_columns as the business keys between base_table (staging) and target_table (presentation)
           tracking deltas via SCD2

        Parameters
        ----------
        hook : PostgresHook
            The Apache Airflow PostgresHook holding the connection details
        target_table : str
            The staging table name
        base_table : str
            The presentation table name
        match_columns : List[str]
            The business keys

        Returns
        -------
        str
            The generated upsert SQL statement
        """        
        # constructs the match predicate, creating the join predicate and combining multiple conditions
        match_predicate: str = '\n and \n'.join(
            ['base.' + column + ' = target.' + column for column in match_columns])

        # fetch the staging columns to automatically generate the insert statement
        clean_columns: List[str] = self._get_insertion_columns(
            hook, 'staging', base_table)
        base_columns: str = ', '.join(
            ['base.' + column for column in clean_columns])

        # add the record valid flags
        clean_columns.extend(['record_start_date', 'record_end_date'])
        insert_columns: str = ','.join(clean_columns)

        # generate the update and insert (upsert) statements
        update_section: str = self._update_statement.format(
            target_update=target_table, target_base=base_table, match_predicate=match_predicate)
        insert_section: str = self._insert_statement.format(target_update=target_table, target_base=base_table, match_predicate=match_predicate, insert_columns=insert_columns,
                                                            base_columns=base_columns)

        query = update_section + insert_section

        return query
