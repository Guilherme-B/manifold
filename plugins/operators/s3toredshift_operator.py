from datetime import datetime

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):

    staging_copy_query = '''
    COPY {destination_name}
    FROM '{source_name}'
        IAM_ROLE '{role_name}'
        FORMAT AS PARQUET;
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 redshift_credentials_id: str,
                 s3_path: str,
                 s3_bucket_template: str,
                 destination_name: str,
                 source_name: str,
                 role_name: str,
                 region_name: str,
                 *args,
                 **kwargs):

        if redshift_conn_id is None or redshift_credentials_id is None or s3_path is None or s3_bucket_template is None or destination_name is None or source_name is None or role_name is None or region_name is None:
            raise ValueError(
                'S3ToRedshiftOperator::__init__ missing arguments')

        if len(redshift_conn_id) == 0 or len(redshift_credentials_id) == 0 or len(s3_path) == 0 or len(s3_bucket_template) == 0 or len(destination_name) == 0 or len(source_name) == 0 or len(role_name) == 0 or len(region_name) == 0:
            raise AttributeError(
                'S3ToRedshiftOperator::__init__ invalid arguments')

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)

        self._redshift_conn_id = redshift_conn_id
        self._redshift_credentials_id = redshift_credentials_id

        self._s3_path = s3_path
        self._s3_bucket_template = s3_bucket_template
        self._source_name = source_name
        self._destination_name = destination_name
        self._region_name = region_name
        self._role_name = role_name

    def execute(self, context):
        execution_date: str = context['execution_date']
        execution_date = execution_date.date()

        bucket_name = self._format_s3_template(
            s3_path=self._s3_path, s3_bucket_template=self._s3_bucket_template, execution_date=execution_date, s3_path_subfolder='tmp')

        query: str = self._format_copy_query(bucket_name=bucket_name, destination_name=self._destination_name,
                                             source_name=self._source_name, role_name=self._role_name, region_name=self._region_name)

        redshift: PostgresHook = PostgresHook(
            postgres_conn_id=self._redshift_conn_id)

        self.log.info(
            'S3ToRedshiftOperator::execute clearing data from Redshift table')

        redshift.run('TRUNCATE {}'.format(self._destination_name))

        self.log.info(
            'S3ToRedshiftOperator::execute copying data from S3 to Redshift')

        redshift.run(query)

        self.log.info(
            'S3ToRedshiftOperator::execute finishing copying data from S3 to Redshift')

    def _format_copy_query(self,
                           bucket_name: str,
                           destination_name: str,
                           source_name: str,
                           role_name: str,
                           region_name: str) -> str:
        """Injects meta data onto the provided copy query

        Parameters
        ----------
        bucket_name : str
            the name of the AWS S3 bucket containing the Parquet files
        destination_name : str
            the AWS Redshift's destination table name
        source_name : str
            the AWS Redshift's source Parquet file name
        role_name : str
            the AWS Redshift's role name
        region_name : str
            the AWS Redshift's region name

        Returns
        -------
        str
            the injected query
        """
        formatted_query: str = self.staging_copy_query.format(
            destination_name=destination_name,
            source_name=source_name.format(bucket_name=bucket_name),
            role_name=role_name,
            region_name=region_name
        )

        return formatted_query

    def _format_s3_template(self,
                            s3_path: str,
                            s3_bucket_template: str,
                            execution_date: str,
                            s3_path_subfolder='tmp') -> str:
        """Generates the full S3 bucket parquet path by injecting the metadata in the S3 template

        Parameters
        ----------
        s3_path : str
            the base s3 path (e.g. s3://manifold_data would be manifold_data)
        s3_bucket_template : str
            the template to define the data location based on the execution_date (e.g. /{week}/{month}/{week}/)
        execution_date : str
            the DAG's execution date
        s3_path_subfolder : str, optional
            the subfolder within the s3_bucket_template which contains the parquet files. Defaults to 'tmp', by default 'tmp'

        Returns
        -------
        str
            the full s3 bucket parquet path
        """

        year: int = execution_date.year
        month: int = execution_date.month
        week: int = execution_date.isocalendar()[1]
        day: int = execution_date.day

        formatted_template: str = s3_bucket_template.format(
            year=year,
            month=month,
            week=week,
            day=day
        )

        full_path: str = 's3://' + s3_path + \
            formatted_template + '/' + s3_path_subfolder + '/'

        return full_path
