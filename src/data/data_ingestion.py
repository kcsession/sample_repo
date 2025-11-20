from pyspark.sql import DataFrame, SparkSession
from src.utils.logging import get_logger

logger = get_logger()

class DataIngestion:
    """
    Handles data ingestion from tables or paths in Databricks.
    """

    def __init__(self, spark: SparkSession):
        logger.info("Data Ingestion started")
        self.spark = spark

    def load_table(self, table_name: str, limit: int = None) -> DataFrame:
        """
        Loads a Spark table with optional limit.
        """
        try:
            logger.info(f"Attempting to load table: {table_name}")
            df = self.spark.table(table_name)

            if limit:
                logger.info(f"Applying limit: {limit}")
                df = df.limit(limit)

            logger.info(f"Successfully loaded table: {table_name}")
            return df

        except ValueError as ve:
            # Raised if limit or parameters are invalid
            logger.error(f"ValueError: Invalid parameter provided. Details: {str(ve)}")
            return None

        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"Unexpected error while loading {table_name}: {type(e).__name__} - {str(e)}")
            return None
