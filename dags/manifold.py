
from datetime import datetime, timedelta
from typing import Dict, List

# airflow
from airflow import DAG

from airflow.utils.dates import days_ago

# access airflow's system-wide config (variables)
#from airflow.configuration import conf
from airflow.models import Variable

# use BaseHook to access Connection credentials
from airflow.hooks.base_hook import BaseHook

# airflow operators
from airflow.operators.bash_operator import BashOperator

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor

# custom operators
from operators.s3toredshift_operator import S3ToRedshiftOperator
from operators.dimension_operator import DimensionOperator

# helpers
from helpers import sql_queries_staging, sql_queries_presentation


# OS variables
start_time = datetime.now()
start_time_str = start_time.strftime("%d/%m/%Y %H:%M:%S")


# Configuration variables
s3_path: str = Variable.get("manifold_s3_path")
s3_path_template: str = Variable.get("manifold_s3_path_template")

aws_connection = BaseHook.get_connection("aws_credentials")
aws_username: str = aws_connection.login
aws_password: str = aws_connection.password

'''

    MANIFOLD DAG CONFIGURATION

'''

########################
# Airflow Dag Configs  #
########################
DEFAULT_ARGS = {
    'owner': 'Guilherme Banhudo',
    'depends_on_past': False,
    'email': ['defaultemail@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

########################
# Manifold EMR Configs #
########################

# the instance type to be spawned
instance_type: str = 'm5.xlarge'


SPARK_STEPS = [
    {
        'Name': 'calculate_pi',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            "Args": [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                f"s3://{s3_path}/scripts/el_to_parquet.py",
                '--execution_date',
                '{{ ds }}',
                '--aws_key',
                aws_username,
                '--aws_secret',
                aws_password,
                '--s3_bucket',
                s3_path,
                '--s3_path_template',
                s3_path_template,
            ],
        },
    }
]


JOB_FLOW_OVERRIDES = {
    'Name': 'Manifold - ' + start_time_str,
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': instance_type,
                'InstanceCount': 1,
            },
            {
                "Name": "Core",
                # Spot instances are a "use as available" instances. Best is ON_DEMAND, but pricier.
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": instance_type,
                "InstanceCount": 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': 's3://base-data-spark-listings/logs/',
    'BootstrapActions': [
        {
            'Name': 'string',
            'ScriptBootstrapAction': {
                'Path': 's3://{{ var.value.s3_path }}/scripts/bootstrap_install_python_modules.sh',
            }
        },
    ],
}


########################
# Start the actual DAG #
########################

with DAG(
    dag_id='manifold_main',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    start_date=days_ago(2),
    schedule_interval='@weekly',
    tags=['manifold'],
) as dag:


    
    '''

        SCRAPERS

    '''

    scrapers_dummy = DummyOperator(task_id='scrapers_dummy',
                                dag=dag
                                )


    '''

        STAGING LAYER PRE-PROCESSING
        (Spark consume Sources save to Parquet)

    '''

    manifold_emr_creator = EmrCreateJobFlowOperator(
        task_id='create_manifold_emr',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_credentials',
        emr_conn_id='emr_credentials',
    )

    manifold_emr_job_sensor = EmrJobFlowSensor(
        task_id='check_emr_completion',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_manifold_emr', key='return_value') }}",
        aws_conn_id='aws_credentials',
    )

    '''

        STAGING TABLE CREATION
        
    '''

    staging_table_creation: List[PostgresOperator] = []
    create_staging_definitions: Dict[str,
                                     str] = sql_queries_staging.create_query_destinition

    # create the staging tables if they do not exist, else truncate
    for object_name, query in create_staging_definitions.items():
        staging_task: PostgresOperator = PostgresOperator(
            task_id=object_name,
            dag=dag,
            postgres_conn_id='redshift_conn',
            sql=query
        )

        staging_table_creation.append(staging_task)

    staging_table_create_dummy = DummyOperator(task_id='staging_table_create_dummy',
                                               dag=dag
                                               )

    '''

        STAGING TABLE POPULATION
        (AWS Redshift COPY)
        
    '''
    # create schema
    staging_schema_creation: PostgresOperator = PostgresOperator(
        task_id='staging_schema_create',
        dag=dag,
        postgres_conn_id='redshift_conn',
        sql=sql_queries_staging.create_staging_schema
    )

    # create the staging tasks

    staging_tasks: List[S3ToRedshiftOperator] = []

    redshift_role_name = Variable.get("manifold_redshift_role_name")
    redshift_region_name = Variable.get("manifold_redshift_region_name")

    # populate the staging layer via Redshift's COPY
    for object_name, config in sql_queries_staging.copy_query_definition.items():
        destination_name: str = config.destination_name
        source_name: str = config.source_name

        operator: S3ToRedshiftOperator = S3ToRedshiftOperator(
            task_id=object_name,
            dag=dag,
            redshift_conn_id='redshift_conn',
            redshift_credentials_id='redshift_credentials',
            s3_path=s3_path,
            s3_bucket_template=s3_path_template,
            destination_name=destination_name,
            source_name=source_name,
            role_name=redshift_role_name,
            region_name=redshift_region_name
        )

        staging_tasks.append(operator)

    staging_table_populate_dummy = DummyOperator(task_id='staging_table_populate_dummy',
                                                 dag=dag
                                                 )

    '''

        PRESENTATION LAYER POPULATION
        (Redshift - Staging to Presentation)
        
    '''

    # create schema
    presentation_schema_creation: PostgresOperator = PostgresOperator(
        task_id='presentation_schema_create',
        dag=dag,
        postgres_conn_id='redshift_conn',
        sql=sql_queries_presentation.create_presentation_schema
    )

    # populate dimensional model
    dimension_definitions: Dict[str, Dict[str, str]
                                ] = sql_queries_presentation.dimension_definitions

    fact_definitions: Dict[str,
                           str] = sql_queries_presentation.fact_definitions

    presentation_dim_tasks: List[DimensionOperator] = []
    presentation_fact_tasks: List[PostgresOperator] = []

    # add the presentation layer's dimension tasks
    for object_name, config in dimension_definitions.items():
        target_table: str = config.get('target_table')
        base_table: str = config.get('base_table')
        match_columns: List[str] = config.get('match_columns')

        presentation_task: DimensionOperator = DimensionOperator(
            task_id=object_name,
            dag=dag,
            postgres_conn_id='redshift_conn',
            target_table=target_table,
            base_table=base_table,
            match_columns=match_columns
        )

        presentation_dim_tasks.append(presentation_task)

    presentation_dimension_dummy = DummyOperator(task_id='presentation_dimension_dummy',
                                                 dag=dag
                                                 )

    # add the presentation layer's fact tasks
    for object_name, query in fact_definitions.items():
        presentation_task: PostgresOperator = PostgresOperator(
            task_id=object_name,
            dag=dag,
            postgres_conn_id='redshift_conn',
            sql=query
        )

        presentation_fact_tasks.append(presentation_task)

    # 2) create the staging Parquet files should the scrapers complete
    scrapers_dummy >> manifold_emr_creator >> manifold_emr_job_sensor

    # 3) create the staging Redshift tables
    manifold_emr_job_sensor >> staging_schema_creation >> staging_table_creation >> staging_table_create_dummy
    	
    # 4) populate the staging Redshift layer if the table creation was successful
    staging_table_create_dummy >> staging_tasks >> staging_table_populate_dummy

    # 5) populate the presentation Redshift layer's dimensions with SCD2
    staging_table_populate_dummy >> presentation_schema_creation >> presentation_dim_tasks >> presentation_dimension_dummy

    # 6) populate the presentation Redshift layer's facts
    presentation_dimension_dummy >> presentation_fact_tasks
    
