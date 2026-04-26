from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_dbutils(spark: SparkSession):
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)
