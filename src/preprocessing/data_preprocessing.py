import pyspark.sql.functions as F
from pyspark.sql import DataFrame

class Preprocessor:
    """
    Cleans raw NYC Taxi trip data.
    """

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Apply cleaning: remove invalid values, fix timestamp types, drop nulls.
        """
        df_clean = (
            df
            .filter("fare_amount > 0")
            .filter("trip_distance > 0")
            .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
            .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
            .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
        )
        return df_clean
