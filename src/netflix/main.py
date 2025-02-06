from utils.etl_functions import read_csv, extract_logger, transform_logger, load_logger, create_postgre_staging, get_postgre_conn
from utils.singletons import Logger

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit

from datetime import datetime
from typing import Optional

logger = Logger('test_netflix_csv').get_logger()
current_date: str = datetime.now().strftime("%Y%m%d")

@extract_logger
def extract(paths: list, date: str = None) -> Optional[dict[str, DataFrame]]:
    datasets: dict[str, DataFrame] = {}

    for path in paths:

        try:
            df = read_csv(path)
            logger.info(f'Extracting dataset: {path.split("/")[2].split(".")[0]}')
            datasets[path.split('/')[2].split('.')[0]] = df
            logger.info('Data successfully extracted.')
        except Exception as e:
            logger.info('Dataset is empty or does not exists.')
            logger.info(e)

    return datasets

@transform_logger
def transform(df_map: dict, date: str) -> Optional[dict[str, DataFrame]]:

    logger.info(f'Current date: {date}')

    df: DataFrame = df_map['netflix_movies']
    
    df_date = df.withColumn('date_issued', lit(date))

    df = df_date.select(
         col('date_issued').cast("int"),
         col('show_id'),
         col('type').alias('media_type'),
         col('title'),
         col('country'),
         col('release_year').cast("int"),
         col('duration'),
         col('listed_in'),
         col('description'),
         col('rating')
    )

    datasets: dict[str, DataFrame] = {
        'df': df
    }

    return datasets

@load_logger
def load(df_dict: dict, load_true: bool = True) -> Optional[DataFrame]:

    df: DataFrame = df_dict['df']
    
    if load_true:
        conn = get_postgre_conn()
        cursor = conn.cursor()

        schema = 'netflix'
        table = 'netflix_media'

        dest_table = f'{schema}.{table}'

        staging_table = create_postgre_staging(df, table, schema)

        # MERGE only works on PostgreSQL 15 or upper (else, use conventional UPSERT)
        merge_query = f"""
        BEGIN;
        MERGE INTO {dest_table} t1
        USING {schema}.{staging_table} t2
        ON t1.show_id = t2.show_id
        AND t1.date_issued = t2.date_issued
        WHEN MATCHED THEN
            UPDATE SET
                media_type = t2.media_type,
                title = t2.title,
                country = t2.country,
                release_year = t2.release_year,
                duration = t2.duration,
                listed_in = t2.listed_in,
                description = t2.description,
                rating = t2.rating
        WHEN NOT MATCHED THEN
            INSERT (date_issued, show_id, media_type, title, country, release_year, duration, listed_in, description, rating)
            VALUES (t2.date_issued, t2.show_id, t2.media_type, t2.title, t2.country, t2.release_year, t2.duration, t2.listed_in, t2.description, t2.rating);
        ANALYZE {dest_table};
        DROP TABLE IF EXISTS {schema}.{staging_table};
        END;
        """

        try:
            pg_version = conn.server_version
            logger.info(f'PG_VERSION: {pg_version}')
            cursor.execute(merge_query)
            conn.commit()
            logger.info(f'Table MERGE successfully completed.')
            #load_postgre(df, schema, table)
        except Exception as e:
            logger.info(f'ERROR: {e}')
        finally:
            cursor.execute(f'DROP TABLE IF EXISTS {schema}.{staging_table};')
            conn.commit()

        cursor.close()
        conn.close()

    logger.info(f'df: {df.show()}')

def main(paths: list):

    extract_res = extract(paths)
    transform_res = transform(extract_res, current_date)
    load(transform_res, load_true=False)

if __name__ == "__main__":
    paths: list = ['./assets/netflix_movies.csv']
    file_type = paths[0].split('/')[2].split('.')[1]

    main(paths)