from utils.etl_functions import read_csv, extract_logger, transform_logger, load_logger
from utils.singletons import Logger, Spark

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DataType

from datetime import datetime

spark: SparkSession = Spark().get_spark_session()
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
    df.createOrReplaceTempView("mock_data")

    durations_df = df.select(
        col("type"),
        col("country"),
        when(length(col("duration")) == 7, col("duration").substr(1, 4).cast("int"))\
            .when(length(col("duration")) < 7, col("duration").substr(1, 3).cast("int"))\
                .otherwise(lit(0).cast("int")).alias("duration")
    )

    durations_grouped_df = durations_df.filter(col("duration") > lit(0))\
        .filter(col("country").isNotNull())\
        .filter(
            ~col("country").like('%,%') & ~col("country").like('%"%') & ~col("country").like('%Cavalieri%') 
        ).groupBy("type", "country", "duration").agg(count("*").alias("total"))
    
    durations_avg_df = durations_grouped_df.withColumn("duration_dr", dense_rank().over(Window.partitionBy("country").orderBy("duration")))
    
    df0 = durations_avg_df.filter(col("duration_dr") == lit(1)).select(
         col("type"), 
         col("country"), 
         col("duration"), 
         col("duration_dr") 
    )
    
    df1 = spark.sql(
        '''
        SELECT 
            type,
            country,
            release_year,
            duration,
            count(*) as total
        FROM mock_data
        WHERE country IS NOT NULL
        AND duration LIKE '%min%'
        GROUP BY 
            type,
            country,
            release_year,
            duration
        HAVING COUNT(*) > 1
        ORDER BY total DESC;
        '''
    )

    df2 = spark.sql(
        '''
        WITH type_totals AS (
            SELECT
                type as total_type,
                COUNT(*) AS total
            FROM mock_data
            WHERE country IS NOT NULL
            GROUP BY type
        )

        SELECT 
            type,
            mock_data.country,
            round((count(*) * 100) / t.total, 2) as percentage
        FROM mock_data
        JOIN type_totals t ON mock_data.type = t.total_type
        WHERE mock_data.country IS NOT NULL
        GROUP BY 
            mock_data.type,
            mock_data.country,
            t.total
        HAVING COUNT(*) > 1
        ORDER BY percentage ASC;
        '''
    )

    df3 = spark.sql(
        '''
        WITH durations AS (
            SELECT
                type,
                country,
                case
                    when length(duration) = 7
                        then cast( substr(duration, 1, 4) as int )
                    when length(duration) < 7
                        then cast( substr(duration, 1, 3) as int )
                    else 0
                end as duration
            FROM mock_data
            WHERE country IS NOT NULL
            and duration like '%min%'
            GROUP BY type, country, duration
        ), durations_avg as (
            SELECT
                type, country,
                AVG(duration) OVER(PARTITION BY country ORDER BY duration ASC) as avg_duration
            FROM durations
            WHERE (country NOT LIKE '%,%' AND country NOT LIKE '%"%')
            GROUP BY type, country, duration
        )

        SELECT
            type,
            country,
            ROUND(avg_duration, 2) as avg_duration
        FROM durations_avg;
        '''
    )

    datasets: dict[str, DataFrame] = {
        'df': df0
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