import SparkSessionWrapper
import pytest


@pytest.fixture(scope='session')
def spark_session():
	environment = 'test'
	spark_lst = SparkSessionWrapper.SparkSession_initialize(environment)
	spark     = spark_lst[0]
    yield spark
    spark.stop()