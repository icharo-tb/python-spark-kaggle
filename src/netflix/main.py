from utils.etl_functions import read_csv, extract_logger, transform_logger, load_logger
from utils.singletons import Logger

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit

from datetime import datetime

logger = Logger('test_netflix_csv').get_logger()
current_date: str = datetime.now().strftime("%Y%m%d")

@extract_logger
def extract(paths: list) -> dict:
    datasets: dict[str, DataFrame] = {}

    for path in paths:

            try:
                df = read_csv(path)
                logger.info(f'Extracting dataset: {path.split("/")[6].split("/")[0]}')
                datasets[path.split('/')[6].split('.')[0]] = df
                logger.info('Data successfully extracted.')
            except Exception as e:
                logger.info('Dataset is empty or does not exists.')
                logger.info(e)

    return datasets

@transform_logger
def transform(df_map: dict, date: str) -> dict:

    logger.info(f'Current date: {date}')

    df: DataFrame = df_map['netflix_movies']
    
    df_date = df.withColumn('date_issued', lit(date))

    df = df_date.select("*")

    datasets: dict[str, DataFrame] = {
        'df': df
    }

    return datasets

@load_logger
def load(df_dict: dict):

    df_users_id: DataFrame = df_dict['df']

    df_users_id.show()

def main(paths: list):

    extract_res = extract(paths)
    transform_res = transform(extract_res, current_date)
    load(transform_res)

if __name__ == "__main__":
    paths: list = [r'/home/daniel-kairos/workspace/python-spark/assets/netflix_movies.csv']
    file_type = paths[0].split('/')[6].split('.')[1]

    main(paths)