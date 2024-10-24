from .singletons import Spark
from pyspark.sql import DataFrame
import kagglehub

from .config import KAGGLE_API_KEY
from .regex_functions import find_csv_files, move_csv_files
from utils.spark_logger import get_logger

logger = get_logger('test_mock_csv')
spark = Spark().get_spark_session()

def read_csv(path: str) -> DataFrame:
    """
    Reads CSV file from path

    Args:
        path (str): Path of the file

    Returns:
        DataFrame: Spark DataFrame of the CSV file
    """
    return spark.read   \
        .option("delimiter", ",") \
        .option("header", True)  \
        .csv(path)

def read_json(path: str) -> DataFrame:
    """
    Reads JSON file from path

    Args:
        path (str): Path of the file

    Returns:
        DataFrame: Spark DataFrame of the JSON file
    """
    return spark.read.json(path)

def extract_kaggle_dataset(name: str, path: str, transfer: bool = True):
    """
    Downloads target Dataset from Kagglehub

    Args:
        name (str): Dataset name on Kaggle
        path (str): Path to the saving folder
        transfer (bool): If set to True, moves file to path

    Returns:
        bool: True if extract was successful, otherwise False
    """
    kaggle_path = kagglehub.dataset_download(name)
    cache_path = '.cache/kagglehub/datasets'

    try:
        if transfer:
            move_csv_files(kaggle_path, path)
            logger.info(f'File moved to: {path}')
        else:
            logger.info(f'Files available at {cache_path}: {find_csv_files(kaggle_path)}')
        logger.info(f'Extract successful from KaggleHub.')
        return True
    except Exception as e:
        logger.info(f'ERROR: {e}')
        return False
    
def extract_logger(func):
    def wrapper(*args, **kwargs):
        logger.info(f"[EXTRACT] Starting extraction for {args} with {kwargs}")
        result = func(*args, **kwargs)
        logger.info(f"[EXTRACT] Completed extraction: {result}")
        return result
    return wrapper

def transform_logger(func):
    def wrapper(*args, **kwargs):
        logger.info(f"[TRANSFORM] Starting transformation for {args} with {kwargs}")
        result = func(*args, **kwargs)
        logger.info(f"[TRANSFORM] Completed transformation: {result}")
        return result
    return wrapper

def load_logger(func):
    def wrapper(*args, **kwargs):
        logger.info(f"[LOAD] Starting loading for {args} with {kwargs}")
        result = func(*args, **kwargs)
        logger.info(f"[LOAD] Completed loading: {result}")
        return result
    return wrapper