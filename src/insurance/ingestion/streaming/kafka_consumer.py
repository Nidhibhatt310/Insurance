from pyspark.sql import SparkSession


class ConsumeKafkaData:
    def __init__(self, spark, dbutils, topic: str, base_location: str):
        self.spark = spark
        self.dbutils = dbutils
        self.topic = topic
        self.base_location = base_location

    def read_from_kafka(self, bootstrap_server: str, kafka_key: str, kafka_secret: str):
        JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_server)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config",
                f'{JAAS_MODULE} required username="{kafka_key}" password="{kafka_secret}";',
            )
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )
        return df

    def write_to_bronze(self, df):
        return (
            df.writeStream.format("delta")
            .option("checkpointLocation", f"{self.base_location}/bronze/{self.topic}")
            .queryName("kafka_bronze_stream")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(f"insurance_dev.bronze.{self.topic}")
        )


def run_kafka_consumer(base_location: str, topic: str = "claims"):
    spark = SparkSession.builder.getOrCreate()

    # safe way to get dbutils inside a wheel
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)

    # secrets fetched inside function — runtime is ready here
    bootstrap_server = dbutils.secrets.get("insurance-kafka-scope-dev", "kafka-bootstrap-server-dev")
    kafka_key = dbutils.secrets.get("insurance-kafka-scope-dev", "kafka-key-dev")
    kafka_secret = dbutils.secrets.get("insurance-kafka-scope-dev", "kafka-secret-dev")

    consumer = ConsumeKafkaData(
        spark=spark,
        dbutils=dbutils,
        topic=topic,
        base_location=base_location,
    )

    df = consumer.read_from_kafka(bootstrap_server, kafka_key, kafka_secret)
    query = consumer.write_to_bronze(df)
    query.awaitTermination()  # ← keeps job alive until stream finishes
