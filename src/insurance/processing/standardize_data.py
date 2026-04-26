from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, from_json

from insurance.utils.config import MetadataConfig, TopicConfig
from insurance.utils.logger import get_logger
from insurance.utils.schema_registry import get_schema

logger = get_logger(__name__)


class BronzeFlattener:
    def __init__(self, spark: SparkSession, config: MetadataConfig):
        self.spark = spark
        self.config = config

    def flatten_topic(self, topic: TopicConfig, audit_logger=None) -> DataFrame:
        source = self.config.bronze_table(topic)
        logger.info("Configuring flatten read: %s (source=%s)", topic.name, source)
        try:
            schema = get_schema(topic.schema_class)
            df = self.spark.readStream.table(source)
            result = (
                df.selectExpr(
                    "CAST(key AS STRING) as key",
                    "CAST(value AS STRING) as raw_json",
                    "topic",
                    "partition",
                    "offset",
                    "timestamp as kafka_timestamp",
                )
                .withColumn(
                    "parsed",
                    from_json(col("raw_json"), schema, {"columnNameOfCorruptRecord": "_rescued_data"}),
                )
                .select("key", "raw_json", "topic", "partition", "offset", "kafka_timestamp", "parsed.*")
            )
            logger.info("Flatten read configured for topic: %s", topic.name)
            if audit_logger:
                self._detect_schema_changes(result, topic, audit_logger)
            return result
        except Exception:
            logger.exception("Failed to configure flatten read for topic: %s", topic.name)
            raise

    def write_flatten(self, df: DataFrame, topic: TopicConfig):
        target = self.config.flatten_table(topic)
        checkpoint = self.config.flatten_checkpoint(topic)
        logger.info("Configuring flatten write: %s → %s", topic.name, target)
        try:
            query = (
                df.writeStream.format("delta")
                .option("checkpointLocation", checkpoint)
                .queryName(f"flatten_{topic.name}")
                .outputMode("append")
                .trigger(availableNow=True)
                .toTable(target)
            )
            logger.info("Flatten stream started: %s (query=%s)", topic.name, query.name)
            return query
        except Exception:
            logger.exception("Failed to start flatten stream for topic: %s", topic.name)
            raise

    def _detect_schema_changes(self, df: DataFrame, topic: TopicConfig, audit_logger) -> None:
        try:
            existing = self.spark.table(self.config.flatten_table(topic))
            existing_fields = {f.name: str(f.dataType) for f in existing.schema}
            new_fields = {f.name: str(f.dataType) for f in df.schema}

            for field in set(new_fields) - set(existing_fields):
                logger.warning("Schema change FIELD_ADDED: %s.%s (%s)", topic.name, field, new_fields[field])
                audit_logger.log_schema_change(topic.name, "FIELD_ADDED", field, new_type=new_fields[field])

            for field in set(existing_fields) - set(new_fields):
                logger.warning("Schema change FIELD_REMOVED: %s.%s (%s)", topic.name, field, existing_fields[field])
                audit_logger.log_schema_change(topic.name, "FIELD_REMOVED", field, old_type=existing_fields[field])

            for field in set(new_fields) & set(existing_fields):
                if new_fields[field] != existing_fields[field]:
                    logger.warning(
                        "Schema change TYPE_CHANGED: %s.%s %s → %s",
                        topic.name, field, existing_fields[field], new_fields[field],
                    )
                    audit_logger.log_schema_change(
                        topic.name, "TYPE_CHANGED", field,
                        old_type=existing_fields[field], new_type=new_fields[field],
                    )
        except AnalysisException:
            pass  # flatten table doesn't exist yet on first run
