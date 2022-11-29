"""
This DAG is responsible for running the sequence of steps from steps_example_dag.


Author: Fabio Barbazza
Date: Oct, 2022
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)




with DAG(dag_id='etl_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:


    create_table = PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql = 'sql_queries/create_tables.sql'
    )    

