"""
This DAG is responsible for running the sequence of steps from steps_example_dag.


Author: Fabio Barbazza
Date: Oct, 2022
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import mlflow
from numpy import random

from tasks.clean_data import CleanData
from tasks.preprocess_data import PreprocessData
from tasks.train_model import TrainModel

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)



mlflow.set_tracking_uri('http://mlflow:600')

experiment = mlflow.set_experiment("MLOps")


def __clean_data(**context):
    """
    This method is responsible for cleaning data
    """
    try:

        logger.info('__clean_data')

        CleanData.run(context)

    except Exception as err:
        logger.exception(err)
        raise err


def __preprocess_data(**context):
    """
    This method is responsible for processing data
    """
    try:

        logger.info('__preprocess_data')

        PreprocessData.run(context)

    except Exception as err:
        logger.exception(err)
        raise err

def __train_model(**context):
    """
    This method is responsible for training model
    """
    try:

        logger.info('__train_model')

        TrainModel.run(context)

    except Exception as err:
        logger.exception(err)
        raise err

with DAG(
    dag_id='mlops_dag', 
    start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', 
    catchup=False) as dag:

    with mlflow.start_run():

        id_run = random.rand()

        mlflow.log_param("run_id_manual",id_run)

        clean_data_task = PythonOperator(
            task_id='t1',
            params={'table':'heart_fact'},
            provide_context=True,
            python_callable=__clean_data
        )


        preprocess_data_task = PythonOperator(
            task_id='t2',
            op_kwargs=dag.default_args,
            provide_context=True,
            python_callable=__preprocess_data
        )

        train_model_task = PythonOperator(
            task_id='t3',
            op_kwargs=dag.default_args,
            provide_context=True,
            python_callable=__train_model
        )

        clean_data_task >> preprocess_data_task >> train_model_task 



    

