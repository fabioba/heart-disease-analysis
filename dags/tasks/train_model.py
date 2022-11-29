"""
This module includes the logic of processing data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from sklearn.linear_model import LogisticRegression

import generic_task
import logging 

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class TrainModel(generic_task.GenericTask):
    """
    
    """

    def __init__(self,**context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing cleaning

        1. read data from PostgreSQL
        2. train model
        3. store data into PostgreSQL
        """
        try:

            x_train = self._get_data('heart_x_train')
            y_train = self._get_data('heart_y_train')
            x_test = self._get_data('heart_x_test')
            y_test = self._get_data('heart_y_test')

            self.__train_model(x_train, y_train, x_test, y_test)

            self._store_data()

        except Exception as err:
            logger.exception(err)
            raise err
    


    def __train_model(self, x_train, y_train, x_test, y_test):
        """
        This method is responsible for training model
        """
        try:


            logger.info('__train_model starting')

            model = LogisticRegression()

            model.fit(x_train,y_train)
            
            y_pred = model.predict(x_test)

            lr_score=model.score(x_test,y_test)*100


            logger.info('__train_model success')

        except Exception as err:
            logger.exception(err)
            raise err

