"""
This DAG is responsible for running the sequence of steps from steps_example_dag.


Author: Fabio Barbazza
Date: Oct, 2022
"""
from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.load_data import LoadDataOperator
from operators.load_dimension_table import LoadDimensionOperator
from airflow.utils.task_group import TaskGroup
from operators.helpers.sql_queries import SqlQueries

import logging

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)




with DAG(
    dag_id='etl_dag', 
    start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', 
    catchup=False) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    # create tables
    create_table = PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql = 'sql_queries/create_tables.sql'
    )    

    # load raw data
    load_raw_data = LoadDataOperator(
        task_id='load_raw_data',
        postgres_conn_id='postgres_default',
        table='raw_heart_disease',
        local_path='data/heart_raw.csv'
    )

    # dimension tables
    with TaskGroup(group_id='load_dim_tables') as load_dim_tables:

        load_account_dim = LoadDimensionOperator(
            sql=SqlQueries.insert_account_dimension,
            table ='account_dim',
            postgres_conn_id='postgres_default',
            task_id='load_account_dim'
        )
        load_heart_dis_dim = LoadDimensionOperator(
            sql=SqlQueries.insert_heart_disease_dimension,
            table ='heart_disease_dim',
            postgres_conn_id='postgres_default',
            task_id='load_heart_dis_dim'
        )

    # fact tables
    load_heart_fact = PostgresOperator(
        task_id="load_heart_fact",
        postgres_conn_id='postgres_default',
        sql=SqlQueries.insert_heart_fact
    )

    start_operator >> create_table >> load_dim_tables >> load_heart_fact