"""
This module includes the logic of cleaning data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from airflow.hooks.postgres_hook import PostgresHook

import logging 
import pandas as pd


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class GenericTask():
    """
    
    """

    def __init__(self,context):
        self.context = context
        self.run_id = context['run_id']

        logger.info('self.run_id: {}'.format(self.run_id))


    def _get_data(self, columns_to_unpack, sql_stmt):
        """
        This method is responsible for reading data

        Args:
            table_name(str)
            columns_to_unpack(list)
            sql_stmt(str)

        Returns:
            read_df(pandas df)
        """
        try:

            logger.info('_get_data starting')

            pg_hook = PostgresHook(
                postgres_conn_id='postgres_default'
            )

            logger.info('sql_stmt: {}'.format(sql_stmt))

            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(sql_stmt)

            read_list = cursor.fetchall()

            read_df = pd.DataFrame(data = read_list, columns = columns_to_unpack)
            logger.info('read_df: {}'.format(read_df.shape))
            #logger.info('read_df shape: {}'.format(read_df.shape))

            logger.info('_get_data success')

            return read_df


        except Exception as err:
            logger.exception(err)
            raise err


    def _store_df(self, df_to_store, table_name, schema_name):
        """
        This method is responsible for storing data

        Args:
            df_to_store(pd dataframe)
            table_name(str)
            schema_name(str)
        """
        try:


            logger.info('_store_df starting')

            table_name_complete = '{}.{}'.format(schema_name, table_name)

            logger.info('table_name complete: {}'.format(table_name_complete))

            postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default') 

            # convert df into rows
            rows = list(df_to_store.itertuples(index=False, name=None))

            tuples = (','.join(str(x) for x in rows))

            logger.info('len tuples:{}'.format(len(tuples)))

            sql_insert = "INSERT INTO {}.{} values {} ON CONFLICT DO NOTHING".format(schema_name, table_name,tuples)


            logger.info('sql_insert: {}'.format(sql_insert))

            postgres_sql_upload.run(sql_insert)

            logger.info('_store_df success')

        except Exception as err:
            logger.exception(err)
            raise err


    def _store_nested_array(self, arr_to_store, table_name, schema_name, handle_duplicates = ''):
        """
        This method is responsible for storing data

        Args:
            arr_to_store(numpy array)
            table_name(str)
            schema_name(str)
            handle_duplicates(str): handle duplicates in storing
        """
        try:


            logger.info('_store_array starting')

            table_name_complete = '{}.{}'.format(schema_name, table_name)

            logger.info('table_name complete: {}'.format(table_name_complete))

            postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default') 

            # convert arr to tuple
            str_arr = [tuple(row) for row in arr_to_store]
            tuples = ','.join(str(x) for x in str_arr)

            logger.info('len tuples:{}'.format(len(tuples)))

            sql_insert = "INSERT INTO {}.{} values {} {}".format(schema_name, table_name,tuples,handle_duplicates)


            logger.info('sql_insert: {}'.format(sql_insert))

            postgres_sql_upload.run(sql_insert)

            logger.info('_store_array success')

        except Exception as err:
            logger.exception(err)
            raise err


    def _store_array(self, arr_to_store, table_name, schema_name, handle_duplicates = ''):
        """
        This method is responsible for storing data

        Args:
            arr_to_store(numpy array)
            table_name(str)
            schema_name(str)
            handle_duplicates(str): handle duplicates in storing
        """
        try:


            logger.info('_store_array starting')

            table_name_complete = '{}.{}'.format(schema_name, table_name)

            logger.info('table_name complete: {}'.format(table_name_complete))

            postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_default') 

            # convert arr to tuple
            tuples = ','.join(str('({})'.format(x)) for x in arr_to_store)

            
            logger.info('len tuples:{}'.format(len(tuples)))

            sql_insert = "INSERT INTO {}.{} values {} {}".format(schema_name, table_name,tuples,handle_duplicates)


            logger.info('sql_insert: {}'.format(sql_insert))

            postgres_sql_upload.run(sql_insert)

            logger.info('_store_array success')

        except Exception as err:
            logger.exception(err)
            raise err
