"""
This module includes the logic for training the model

Date: Oct, 2022
Author: Fabio Barbazza
"""
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

import mlflow

from tasks import generic_task
import logging 

logger = logging.getLogger(__name__)

class TrainModel(generic_task.GenericTask):
    """
    """

    def __init__(self,context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing training of the model

        1. read data from PostgreSQL
        2. train model
        3. store data into PostgreSQL
        """
        try:
            columns_to_unpack_x = ["age" , "sex" , "cp" ,
                                                                "trestbps" ,
                                                                "chol" ,
                                                                "fbs" ,
                                                                "restecg" ,
                                                                "thalach" ,
                                                                "exang" ,
                                                                "oldpeak" ,
                                                                "slope" ,
                                                                "ca" ,
                                                                "thal"]
            columns_to_unpack_y= ["target"]

            sql_x_train = "SELECT * FROM heart_analysis.heart_x_train"
            x_train = self._get_data(columns_to_unpack_x, sql_x_train)

            sql_y_train = "SELECT * FROM heart_analysis.heart_y_train"
            y_train = self._get_data(columns_to_unpack_y, sql_y_train)


            self.__train_model(x_train, y_train)

        except Exception as err:
            logger.exception(err)
            raise err
    


    def __train_model(self, x_train, y_train):
        """
        This method is responsible for training model

        Args:  
            x_train(data)
            y_train(data)
        """
        try:

            experiment = mlflow.set_experiment("train_model")

            grid_param = {
                'n_estimators': [5, 10, 20, 50],
                'criterion': ['gini', 'entropy'],
                'bootstrap': [True, False]
            }

            with mlflow.start_run():

                logger.info('__train_model starting')

                classifier = RandomForestClassifier(random_state=0)

                gd_sr = GridSearchCV(estimator=classifier,
                     param_grid=grid_param,
                     scoring='accuracy',
                     cv=5,
                     n_jobs=-1)

                gd_sr.fit(x_train,y_train)

                best_parameters = gd_sr.best_params_
                logger.info('best_parameters: {}'.format(best_parameters))

                best_bootstrap = best_parameters['bootstrap']
                best_criterion = best_parameters['criterion']
                best_n_estimators = best_parameters['n_estimators']

                mlflow.log_param("model_type",'RandomForest')
                mlflow.log_metric("bootstrap",best_bootstrap)
                mlflow.log_param("criterion",best_criterion)
                mlflow.log_metric("n_estimators",best_n_estimators)

                classifier = RandomForestClassifier(random_state=0, bootstrap = best_bootstrap, criterion = best_criterion, n_estimators= best_n_estimators)
                classifier.fit(x_train, y_train)

                mlflow.sklearn.save_model(sk_model = classifier, path ='mlruns/{}.pkl'.format(self.run_id))

                logger.info('saved model')

                # this need to be reviewed
                mlflow.sklearn.log_model(
                    sk_model=classifier,
                    artifact_path="mlruns/test2.pkl",
                    registered_model_name="sk-learn-random-forest-reg-model"
                )


            logger.info('__train_model success')

        except Exception as err:
            logger.exception(err)
            raise err

