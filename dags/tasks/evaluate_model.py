"""
This module includes the logic for evaluating the pre-trained model

Date: Oct, 2022
Author: Fabio Barbazza
"""
import mlflow
from sklearn.metrics import accuracy_score

from tasks import generic_task
import logging 

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

class EvaluateModel(generic_task.GenericTask):
    """
    """

    def __init__(self,context):
        generic_task.GenericTask.__init__(self, context)


    def run(self):
        """
        This method is responsible for performing evaluation of the model

        1. read test dataset from PostgreSQL
        2. evaluate model
        3. store evaluation into PostgreSQL
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

            sql_x_test = "SELECT * FROM heart_analysis.heart_x_test"
            x_test = self._get_data(columns_to_unpack_x, sql_x_test)

            sql_y_test = "SELECT * FROM heart_analysis.heart_y_test"
            y_test = self._get_data(columns_to_unpack_y, sql_y_test)

            self.__evaluate_model(x_test, y_test)

        except Exception as err:
            logger.exception(err)
            raise err
    


    def __evaluate_model(self,model, x_test, y_test):
        """
        This method is responsible for evaluating the model

        Args:
            model(obj)
            x_test(data)
            y_test(data)
        """
        try:

            experiment = mlflow.set_experiment("evaluate_model")

            with mlflow.start_run():

                logger.info('__evaluate_model starting')
                
                y_pred = model.predict(x_test)

                acc = accuracy_score(y_test, y_pred)

                mlflow.log_param("model_type",'RandomForest')
                mlflow.log_metric("accuracy",acc)


            logger.info('__evaluate_model success')

        except Exception as err:
            logger.exception(err)
            raise err

