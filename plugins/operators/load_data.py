"""
This operator is responsible for loading data from local storage to Postgres table

Date: Nov, 2022
Author: Fabio Barbazza
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class LoadDataOperator(BaseOperator):
    """
    This class performs loading operation on input data
    """

    copy_sql_from_csv = """
                COPY {}
                FROM '{}'
                IGNOREHEADER 1
                DELIMITER ';'
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql="",
                 table="",
                 local_path="",
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.local_path = local_path

    def execute(self, context):
        """
        Execute
        """
        try:

            self.log.info('connect to postgres')

            postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

            formatted_sql = LoadDataOperator.copy_sql_from_csv.format(
                self.table,
                self.local_path
            )

            self.log.info('formatted_sql: {}'.format(formatted_sql))

            postgres.run(formatted_sql)

            #creating_hear_dis_raw_table = PostgresOperator(
            #    task_id="creating_experiment_tracking_table",
            #    postgres_conn_id='postgres_default',
            #    sql='sql/create_experiments.sql',
            #    table = ''
            #)

        except Exception as err:
            self.log.exception(err)
            raise err

