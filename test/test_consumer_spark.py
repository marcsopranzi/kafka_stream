import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fn
from unittest.mock import patch
from ..consumer_spark import write_to_cassandra


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.appName("pytest-spark").master("local[2]").getOrCreate()
    )
    yield spark
    spark.stop()


def test_write_to_cassandra(spark_session):
    spark = spark_session

    sample_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    sample_df = spark.createDataFrame(sample_data, ["id", "name"])

    with patch("pyspark.sql.DataFrameWriter.save") as mock_save:
        write_to_cassandra(sample_df, batch_id=123)
        mock_save.assert_called_once_with()
