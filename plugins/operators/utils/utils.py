"""
The aim of this module is to provide generic function to support DAGS executions


Author: Fabio Barbazza
Date: Feb, 2023
"""
import logging
import yaml

logger = logging.getLogger(__name__)

def get_config(path_config_file):
    """
    The aim of this method is to return the content of a config file

    Args:
        path_config_file(str): path of the config file

    Returns:
        config_unpacked(str): content of the config file
    """
    try:


        with open(path_config_file) as file:
            config_unpacked = yaml.load(file, Loader=yaml.FullLoader)

        
        return config_unpacked

    except Exception as err:
        logger.exception(err)
        raise err
