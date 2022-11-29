"""
This module includes the logic of cleaning data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from tasks import generic_task
import logging 

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class CleanData(generic_task.GenericTask):
    """
    """

    def __init__(self,**context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing cleaning

        1. read data from PostgreSQL
        2. clean data
        3. store data
        """
        try:

            self.heart_fact = self._get_data('heart_fact')

            self.__clean_data()

            self._store_data(self.heart_fact,'heart_fact_cleaned')

        except Exception as err:
            logger.exception(err)
            raise err
    


    def __clean_data(self):
        """
        This method is responsible for performing cleaning
        """
        try:


            logger.info('__clean_data starting')

            self.heart_fact = self.heart_fact.drop_duplicates()

            logger.info('__clean_data success')

        except Exception as err:
            logger.exception(err)
            raise err

