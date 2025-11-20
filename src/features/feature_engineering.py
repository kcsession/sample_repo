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
        try:
            # Convert to pandas API on Spark
            psdf = df.pandas_api()

            # Ensure datetime conversion
            psdf["tpep_pickup_datetime"] = ps.to_datetime(psdf["tpep_pickup_datetime"])

            # Feature engineering
            psdf["hour"] = psdf["tpep_pickup_datetime"].dt.hour
            psdf["day"] = psdf["tpep_pickup_datetime"].dt.dayofweek
            psdf["month"] = psdf["tpep_pickup_datetime"].dt.month

            return psdf

        except KeyError as ke:
            # Raised if the column doesn't exist
            print(f"KeyError: Missing column in DataFrame. Details: {str(ke)}")
            return None

        except TypeError as te:
            # Raised if datetime conversion fails due to wrong type
            print(f"TypeError: Datetime conversion failed. Details: {str(te)}")
            return None

        except Exception as e:
            # Catch-all for unexpected errors
            print(f"Unexpected error in FeatureEngineer: {type(e).__name__} - {str(e)}")
            return None
