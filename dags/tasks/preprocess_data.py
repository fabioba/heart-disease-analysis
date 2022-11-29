"""
This module includes the logic of processing data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from airflow.hooks.postgres_hook import PostgresHook
import generic_task
import logging 

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class PreprocessData(generic_task.GenericTask):
    """
    
    """

    def __init__(self,**context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing cleaning

        1. read data from PostgreSQL
        2. pre-process data
        3. store data
        """
        try:

            self._read_data()

            self.__process_data()

            self._store_data()

        except Exception as err:
            logger.exception(err)
            raise err
    


    def __process_data(self):
        """
        This method is responsible for performing pre-processing
        """
        try:


            logger.info('__process_data starting')

            self.heart_fact = self.heart_fact.drop_duplicates()

            logger.info('__process_data success')

        except Exception as err:
            logger.exception(err)
            raise err

