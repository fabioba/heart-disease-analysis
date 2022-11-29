"""
This module includes the logic of cleaning data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from airflow.hooks.postgres_hook import PostgresHook

import logging 

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class GenericTask():
    """
    
    """

    def __init__(self,**context):
        self.table = context["params"]["table"]

        logger.info('self.table')


    def _read_data(self):
        """
        This method is responsible for reading data
        """
        try:

            logger.info('__read_data starting')

            sql_stmt = "SELECT * FROM {}".format(self.table)
            pg_hook = PostgresHook(
                postgres_conn_id='postgres_default',
                schema='public'
            )
            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(sql_stmt)

            self.heart_fact = cursor.fetchall()

            logger.info('heart_fact shape: {}'.format(self.heart_fact.shape))

            logger.info('__read_data success')


        except Exception as err:
            logger.exception(err)
            raise err


    def _store_data(self):
        """
        This method is responsible for storing data
        """
        try:


            logger.info('__store_data starting')

            postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default', schema='public') 
            postgres_sql_upload.insert_rows('heart_fact_cleaned', self.heart_fact)


            logger.info('__store_data success')

        except Exception as err:
            logger.exception(err)
            raise err
