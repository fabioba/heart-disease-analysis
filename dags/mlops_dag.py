"""
This DAG is responsible for running the sequence of steps from steps_example_dag.


Author: Fabio Barbazza
Date: Oct, 2022
"""
import logging
FORMAT = '%(asctime)s - %(message)s'
logging.basicConfig(format = FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

from tasks.clean_data import CleanData
from tasks.preprocess_data import PreprocessData
from tasks.train_model import TrainModel

import mlflow
mlflow.set_tracking_uri('http://mlflow:600')

from operators.utils.utils import get_config

default_args = {
    'owner': 'Fabio Barbazza',
    'start_date': datetime(2022, 1, 1), 
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 

config = get_config('config/config.yaml')


def __clean_data(**context):
    """
    This method is responsible for cleaning data
    """
    try:

        logger.info('__clean_data')

        c_data = CleanData(context=context)
        c_data.run()

    except Exception as err:
        logger.exception(err)
        raise err


def __preprocess_data(**context):
    """
    This method is responsible for processing data
    """
    try:

        logger.info('__preprocess_data')

        p_data = PreprocessData(context=context)
        p_data.run()

    except Exception as err:
        logger.exception(err)
        raise err

def __train_model(**context):
    """
    This method is responsible for training model
    """
    try:

        logger.info('__train_model')

        t_model = TrainModel(context=context)
        t_model.run()

    except Exception as err:
        logger.exception(err)
        raise err

        

with DAG(
        dag_id = 'mlops_dag',
        default_args = default_args,
        config = config
    ) as dag:


    # create tables
    create_table = PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql = 'sql_queries/create_tables_ml.sql'
    )    
    


    clean_data_task = PythonOperator(
        task_id='clean_data',
        provide_context=True,
        python_callable=__clean_data
    )


    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        provide_context=True,
        python_callable=__preprocess_data
    )

    train_model_task = PythonOperator(
        task_id='train_model',
        provide_context=True,
        python_callable=__train_model
    )

    create_table >> clean_data_task >> preprocess_data_task >> train_model_task 



    

