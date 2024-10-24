from pyspark.sql import SparkSession
import logging

class Spark:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Spark, cls).__new__(cls)
            cls._instance.spark = SparkSession.builder.appName("Test").getOrCreate()
        return cls._instance

    def get_spark_session(self):
        return self.spark

class Logger:
    _instance = None

    def __new__(cls, name: str):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance.logger = cls._create_logger(name)
        return cls._instance

    @staticmethod
    def _create_logger(name: str):
        # Create a logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # Create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add formatter to ch
        ch.setFormatter(formatter)

        # Add ch to logger
        logger.addHandler(ch)

        return logger

    def get_logger(self):
        return self.logger