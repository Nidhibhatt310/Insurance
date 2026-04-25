from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json
from insurance.utils.schema_registry import get_schema


class FlattenTopics:
    def __init__(self, env: str, topic: str, schema: StructType, spark: SparkSession):
        self.env = env
        self.topic = topic
        self.schema = schema
        self.spark = spark

    def standardize_topic(self) -> DataFrame:
        """
        Standardizes raw Kafka data by parsing JSON payload, flattening fields,
        and capturing unexpected or corrupt data.

        Returns:
            DataFrame: Standardized DataFrame with:
                - Parsed columns based on schema
                - Original raw JSON column ('json')
                - '_rescued_data' column for unexpected/malformed fields

        Notes:
            - Ensures schema evolution handling by preserving raw JSON.
            - Unknown fields are captured in '_rescued_data'.
        """
        try:
            df = self.spark.readStream.table(f"insurance_{self.env}.bronze.{self.topic}")

            df_parsed = (
                df.selectExpr(
                    "CAST(key AS STRING) as key",
                    "CAST(value AS STRING) as raw_json",
                    "topic",
                    "partition",
                    "offset",
                    "timestamp as kafka_timestamp",
                )
                .withColumn(
                    "parsed", from_json(col("raw_json"), self.schema, {"columnNameOfCorruptRecord": "_rescued_data"})
                )
                .select("key", "raw_json", "topic", "partition", "offset", "kafka_timestamp", "parsed.*")
            )

            return df_parsed

        except Exception as e:
            # Logging will be plugged in later
            print(f"Error in standardizing topic data: {str(e)}")
            raise

    def write_standardized(self, df: DataFrame, base_location: str):

        return (
            df.writeStream.format("delta")
            .option("checkpointLocation", f"{base_location}/flatten/{self.topic}")
            .queryName(f"Flatten_{self.topic}_stream")
            .outputMode("append")
            .trigger(availableNow=True)  # or processingTime later
            .toTable(f"insurance_{self.env}.flatten.{self.topic}")
        )


def run_flattening_pipeline(base_location: str, env: str, topic: str):
    spark = SparkSession.builder.getOrCreate()
    schema_defined = get_schema(topic)

    flattener = FlattenTopics(env=env, topic=topic, schema=schema_defined, spark=spark)
    standardized_df = flattener.standardize_topic()
    query = flattener.write_standardized(standardized_df, base_location)
    query.awaitTermination()  # ← keeps job alive until stream finishes
