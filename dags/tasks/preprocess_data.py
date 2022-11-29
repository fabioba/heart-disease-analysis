"""
This module includes the logic of processing data

Date: Oct, 2022
Author: Fabio Barbazza
"""
from sklearn import preprocessing
from sklearn.model_selection import train_test_split

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

            self.heart_fact = self._get_data('heart_fact_cleaned')

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

            all_classes = self.heart_fact['target'].values

            all_features = self.heart_fact[['age', 'sex', 'cp', 'trestbps', 'chol', 'fbs', 'restecg', 'thalach',
                'exang', 'oldpeak', 'slope', 'ca', 'thal']].values

            scaler = preprocessing.StandardScaler()
            all_features_scaled = scaler.fit_transform(all_features)

            (x_train, x_test, y_train, y_test) = train_test_split(all_features_scaled, all_classes,test_size=0.70, random_state=0)
            
            self._store_data(x_train, 'heart_x_train')
            self._store_data(x_test, 'heart_x_test')
            self._store_data(y_train, 'heart_y_train')
            self._store_data(y_test, 'heart_y_test')


            logger.info('__process_data success')

        except Exception as err:
            logger.exception(err)
            raise err

