import os
import joblib
import numpy as np
import pyspark.sql.functions as F
import pyspark.pandas as ps

from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from src.utils.logging import get_logger

logger = get_logger(__name__)

def prepare_features(df_spark):
    # Clean data
    df_clean = (
        df_spark
        .filter("fare_amount > 0")
        .filter("trip_distance > 0")
        .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    )

    # Convert to pandas-on-Spark
    psdf = df_clean.pandas_api()
    psdf["tpep_pickup_datetime"] = ps.to_datetime(psdf["tpep_pickup_datetime"])
    psdf["hour"] = psdf["tpep_pickup_datetime"].dt.hour
    psdf["day"] = psdf["tpep_pickup_datetime"].dt.dayofweek
    psdf["month"] = psdf["tpep_pickup_datetime"].dt.month

    # Select features
    model_psdf = psdf[["fare_amount", "trip_distance", "hour", "day", "month"]].dropna()
    return model_psdf.to_pandas()

def train_model(pdf):
    X = pdf[["trip_distance", "hour", "day"]]
    y = pdf["fare_amount"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=14,
        random_state=42
    )
    model.fit(X_train, y_train)

    # Save model and test set for evaluation
    os.makedirs("artefacts/models", exist_ok=True)
    joblib.dump(model, "artefacts/models/random_forest.pkl")
    joblib.dump((X_test, y_test), "artefacts/models/test_data.pkl")

    logger.info("Model training complete. Saved to artefacts/models/")
    return model

def main():
    spark = SparkSession.builder.appName("Training").getOrCreate()
    df = spark.table("samples.nyctaxi.trips").limit(200)

    pdf = prepare_features(df)
    train_model(pdf)

if __name__ == "__main__":
    main()
