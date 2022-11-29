"""
This operator is responsible for loading data from local storage to Postgres table

Date: Nov, 2022
Author: Fabio Barbazza
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import BaseOperator


class LoadDataOperator(BaseOperator):
    """
    This class performs loading operation on input data
    """
    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        """
        Execute
        """
        try:

            creating_experiment_tracking_table = PostgresOperator(
                task_id="creating_experiment_tracking_table",
                postgres_conn_id='postgres_default',
                sql='sql/create_experiments.sql',
                table = ''
            )

        except Exception as err:
            self.log.exception(err)
            raise err

