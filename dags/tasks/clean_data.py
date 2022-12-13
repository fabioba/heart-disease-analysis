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

    def __init__(self,context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing cleaning

        1. read data from PostgreSQL
        2. clean data
        3. store data
        """
        try:

            columns_to_unpack = ["account_id" ,"age" , "sex" , "cp" ,
                                                                "trestbps" ,
                                                                "chol" ,
                                                                "fbs" ,
                                                                "restecg" ,
                                                                "thalach" ,
                                                                "exang" ,
                                                                "oldpeak" ,
                                                                "slope" ,
                                                                "ca" ,
                                                                "thal" ,
                                                                "target"]

            sql_stmt = "SELECT * FROM heart_analysis.heart_fact"

            self.heart_fact = self._get_data(columns_to_unpack, sql_stmt)

            self.__clean_data()

            self.__add_pipeline_run()

            self._store_df(self.heart_fact,'heart_fact_cleaned','heart_analysis')

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


    def __add_pipeline_run(self):
        """
        Add pipeline run feature on dataframe
        """
        try:


            logger.info('__add_pipeline_run starting')

            self.heart_fact['pipeline_run'] = self.run_id

            logger.info('__add_pipeline_run success')

        except Exception as err:
            logger.exception(err)
            raise err

