from pyspark.sql import DataFrame, SparkSession

from insurance.utils.config import KafkaCreds, MetadataConfig, TopicConfig
from insurance.utils.logger import get_logger

logger = get_logger(__name__)

_JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"


class KafkaBronzeIngester:
    def __init__(self, spark: SparkSession, config: MetadataConfig, creds: KafkaCreds):
        self.spark = spark
        self.config = config
        self.creds = creds

    def _read_topic(self, topic: TopicConfig) -> DataFrame:
        try:
            return (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.creds.bootstrap_server)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option(
                    "kafka.sasl.jaas.config",
                    f'{_JAAS_MODULE} required username="{self.creds.sasl_key}" password="{self.creds.sasl_secret}";',
                )
                .option("subscribe", topic.kafka_topic)
                .option("startingOffsets", "earliest")
                .load()
            )
        except Exception:
            logger.exception("Failed to read Kafka topic: %s", topic.kafka_topic)
            raise

    def ingest_topic(self, topic: TopicConfig):
        target = self.config.bronze_table(topic)
        logger.info("Configuring stream: %s → %s", topic.kafka_topic, target)
        try:
            df = self._read_topic(topic)
            query = (
                df.writeStream.format("delta")
                .option("checkpointLocation", self.config.bronze_checkpoint(topic))
                .queryName(f"kafka_bronze_{topic.name}")
                .outputMode("append")
                .trigger(availableNow=True)
                .toTable(target)
            )
            logger.info("Stream started: %s (query=%s)", topic.name, query.name)
            return query
        except Exception:
            logger.exception("Failed to start stream for topic: %s", topic.name)
            raise


def get_kafka_creds(dbutils, kafka_cfg: dict) -> KafkaCreds:
    scope = kafka_cfg["secrets_scope"]
    logger.info("Fetching Kafka credentials from secrets scope: %s", scope)
    try:
        creds = KafkaCreds(
            bootstrap_server=dbutils.secrets.get(scope, kafka_cfg["bootstrap_server_key"]),
            sasl_key=dbutils.secrets.get(scope, kafka_cfg["sasl_key"]),
            sasl_secret=dbutils.secrets.get(scope, kafka_cfg["sasl_secret_key"]),
        )
        logger.info("Kafka credentials fetched successfully")
        return creds
    except Exception:
        logger.exception("Failed to fetch Kafka credentials from scope: %s", scope)
        raise
