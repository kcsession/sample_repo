%pip install pytest

import pytest
from pyspark.sql import SparkSession
from src.data.data_ingestion import DataIngestion   # adjust import path if needed

@pytest.fixture(scope="session")
def spark():
    # Create a local SparkSession for testing
    return SparkSession.builder \
        .appName("pytest-data-ingestion") \
        .master("local[1]") \
        .getOrCreate()

def test_load_table_without_limit(spark):
    # Create a temporary table
    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.createOrReplaceTempView("people")

    ingestion = DataIngestion(spark)
    result = ingestion.load_table("people")

    # Assertions
    assert result.count() == 2
    assert set(result.columns) == {"name", "age"}

def test_load_table_with_limit(spark):
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.createOrReplaceTempView("people_limit")

    ingestion = DataIngestion(spark)
    result = ingestion.load_table("people_limit", limit=2)

    # Assertions
    assert result.count() == 2
    assert set(result.columns) == {"name", "age"}
