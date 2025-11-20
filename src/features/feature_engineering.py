import pyspark.pandas as ps
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class FeatureEngineer:
    """
    Creates derived time-based features for ML models.
    """

    def add_time_features(self, df: DataFrame) -> ps.DataFrame:
        """
        Converts Spark DataFrame to pandas-on-Spark DataFrame and adds hour/day/month features.
        """

        # Convert to pandas API on Spark
        psdf = df.pandas_api()

        # Ensure datetime conversion
        psdf["tpep_pickup_datetime"] = ps.to_datetime(psdf["tpep_pickup_datetime"])

        # Feature engineering
        psdf["hour"] = psdf["tpep_pickup_datetime"].dt.hour
        psdf["day"] = psdf["tpep_pickup_datetime"].dt.dayofweek
        psdf["month"] = psdf["tpep_pickup_datetime"].dt.month

        return psdf

