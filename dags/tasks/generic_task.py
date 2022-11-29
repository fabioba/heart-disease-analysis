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


    def _get_data(self, table_name):
        """
        This method is responsible for reading data

        Returns:
            read_df(pandas df)
        """
        try:

            logger.info('__read_data starting')

            sql_stmt = "SELECT * FROM {}".format(table_name)
            pg_hook = PostgresHook(
                postgres_conn_id='postgres_default',
                schema='public'
            )
            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(sql_stmt)

            read_df = cursor.fetchall()

            logger.info('read_df shape: {}'.format(read_df.shape))

            logger.info('__read_data success')

            return read_df


        except Exception as err:
            logger.exception(err)
            raise err


    def _store_data(self, df_to_store, table_name):
        """
        This method is responsible for storing data

        Args:
            df_to_store(pd dataframe)
            table_name(str)
        """
        try:


            logger.info('__store_data starting')

            postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default', schema='public') 
            postgres_sql_upload.insert_rows(table_name, df_to_store)


            logger.info('__store_data success')

        except Exception as err:
            logger.exception(err)
            raise err
