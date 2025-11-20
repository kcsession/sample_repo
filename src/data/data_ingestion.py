from pyspark.sql import DataFrame, SparkSession
from src.utils.logging import get_logger

logger = get_logger()

class DataIngestion:
    """
    Handles data ingestion from tables or paths in Databricks.
    """

    def __init__(self, spark: SparkSession):
        logger.info("Data Ingestion stsrted")
        self.spark = spark

    def load_table(self, table_name: str, limit: int = None) -> DataFrame:
        """
        Loads a Spark table with optional limit.
        """
        df = self.spark.table(table_name)
        if limit:
            df = df.limit(limit)
        return df
