"""
This operator is responsible for loading data from local storage to Postgres table

Date: Nov, 2022
Author: Fabio Barbazza
"""
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
import os
import pandas as pd


class LoadDataOperator(BaseOperator):
    """
    This class performs loading operation on input data
    """

    copy_sql_from_csv = """
                COPY {}
                FROM '{}'
                DELIMITER ','
                CSV HEADER;
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

            df_input_table = pd.read_csv(self.local_path, delimiter=';')
            self.log.info('df_input_table: {}'.format(df_input_table.shape))

            postgres = PostgresHook(postgres_conn_id = self.postgres_conn_id)

            formatted_sql = LoadDataOperator.copy_sql_from_csv.format(
                self.table,
                self.local_path
            )

            self.log.info('formatted_sql: {}'.format(formatted_sql))
            self.log.info('os.getcwd(): {}'.format(os.getcwd()))

            # convert into rows
            rows = list(df_input_table.itertuples(index=False, name=None))
            postgres.insert_rows(table = self.table, rows=rows)

            #postgres.run(formatted_sql)

            #creating_hear_dis_raw_table = PostgresOperator(
            #    task_id="creating_experiment_tracking_table",
            #    postgres_conn_id='postgres_default',
            #    sql='sql/create_experiments.sql',
            #    table = ''
            #)

        except Exception as err:
            self.log.exception(err)
            raise err

