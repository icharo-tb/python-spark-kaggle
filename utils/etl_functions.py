from .singletons import Spark
from pyspark.sql import DataFrame
import kagglehub
import psycopg2

from .config import load_config
from .regex_functions import find_csv_files, move_csv_files
from utils.spark_logger import get_logger

import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional

load_dotenv

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

def load_postgre(df: DataFrame, schema: str, table: str) -> str:

    try:
        df.write \
            .format("jdbc") \
            .option("url", os.getenv('POSTGRE_URL')) \
            .option("dbtable", f'{schema}.{table}') \
            .option("user", os.getenv('POSTGRE_USER')) \
            .option("password", os.getenv('POSTGRE_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append")\
            .save()
        
        logger.info(f'Data loaded to PostgreSQL successfully.')
    except Exception as e:
        logger.info(f'Could not load table into {schema}.{table}\nERROR: {e}')

def create_postgre_staging(df: DataFrame, base_table_name: str, db_schema: str) -> str:

    """
    Create and return a PostgreSQL staging table name.

    Args:
        df (DataFrame): Table dataframe
        base_table_name (str): Base dataframe table name
        db_schema (str): PostgreSQL schema
    
    Returns:
        str: Staging table name
    """

    conf = load_config()

    # Create a connection to PostgreSQL
    conn = psycopg2.connect(**conf)
    cursor = conn.cursor()
    
    # Generate a new table name based on the current date
    current_date = datetime.now().strftime("%Y_%m_%d")
    staging_table_name = f"_staging_{base_table_name}_{current_date}"
    
    # Create the staging table SQL statement based on the DataFrame schema
    schema = df.schema
    columns = []
    
    for field in schema.fields:
        # Map PySpark data types to PostgreSQL types
        if field.dataType.typeName() == 'string':
            columns.append(f"{field.name} VARCHAR")
        elif field.dataType.typeName() == 'integer':
            columns.append(f"{field.name} INTEGER")
        elif field.dataType.typeName() == 'double':
            columns.append(f"{field.name} DOUBLE PRECISION")
        elif field.dataType.typeName() == 'date':
            columns.append(f"{field.name} DATE")
        # Add more data types as needed

    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.{staging_table_name} (
            {', '.join(columns)}
        );
        """
    
        # Execute the create table query
        cursor.execute(create_table_query)
        conn.commit()

        # Write DataFrame to the new staging table
        df.write \
            .format("jdbc") \
            .option("url", os.getenv('POSTGRE_URL')) \
            .option("dbtable", f'{db_schema}.{staging_table_name}') \
            .option("user", os.getenv('POSTGRE_USER')) \
            .option("password", os.getenv('POSTGRE_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append")\
            .save()
    except Exception as e:
        logger.info(f'Staging table {db_schema}.{staging_table_name} already exists.\nERROR: {e}')
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    return staging_table_name

def get_postgre_conn() -> Optional[psycopg2.extensions.connection]:

    """
    Create and return a PostgreSQL database connection.

    Return:
        Optional[psycopg2.extensions.connection]: psycopg2 connection object
    """

    try:
        conf = load_config()

        # Create a connection to PostgreSQL
        conn = psycopg2.connect(**conf)

        return conn
    except Exception as e:
        logger.info(f"Could not retrieve PostgreSQl conn.\nERROR: {e}")
        return None