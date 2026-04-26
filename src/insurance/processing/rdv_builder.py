from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from insurance.utils.config import (
    HubConfig,
    LinkConfig,
    MetadataConfig,
    SatelliteConfig,
    TopicConfig,
)
from insurance.utils.constants import HASH_DIFF_COL, LOAD_DATE_COL, RECORD_SOURCE, RECORD_SOURCE_COL
from insurance.utils.logger import get_logger

logger = get_logger(__name__)


class RDVBuilder:
    """
    Generic Raw Data Vault engine. Reads topic metadata from MetadataConfig
    and builds Hub / Satellite / Link tables using Delta MERGE for idempotent loads.
    Adding a new topic requires only a YAML config change — no code changes needed.
    """

    def __init__(self, spark: SparkSession, config: MetadataConfig, audit_logger=None):
        self.spark = spark
        self.config = config
        self.audit_logger = audit_logger

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _flatten_df(self, topic: TopicConfig):
        return self.spark.table(self.config.flatten_table(topic))

    def _ensure_table(self, df, table_name: str) -> None:
        """Create an empty Delta table from df schema if it does not yet exist."""
        try:
            self.spark.table(table_name)
        except AnalysisException:
            logger.info("Table %s does not exist — creating empty Delta table", table_name)
            try:
                df.limit(0).write.format("delta").saveAsTable(table_name)
                logger.info("Created table: %s", table_name)
            except Exception:
                logger.exception("Failed to create table: %s", table_name)
                raise

    def _merge_metrics(self, table_name: str) -> tuple:
        """Read inserted/updated row counts from the last Delta MERGE via table history."""
        try:
            row = self.spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
            m = row["operationMetrics"]
            return int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0))
        except Exception:
            logger.warning("Could not retrieve MERGE metrics for %s", table_name)
            return 0, 0

    # ------------------------------------------------------------------
    # Hub
    # ------------------------------------------------------------------

    def build_hub(self, topic: TopicConfig) -> None:
        hub: HubConfig = topic.rdv.hub
        if hub is None:
            return
        hub_table = self.config.rdv_table(hub.table)
        logger.info("Building hub: %s → %s", hub.name, hub_table)
        start = datetime.now(timezone.utc)
        try:
            df = self._flatten_df(topic)
            records_in = df.count() if self.audit_logger else None
            hub_df = (
                df.select(
                    F.md5(F.col(hub.business_key).cast("string")).alias(hub.hash_key),
                    F.col(hub.business_key),
                    F.current_timestamp().alias(LOAD_DATE_COL),
                    F.lit(RECORD_SOURCE).alias(RECORD_SOURCE_COL),
                )
                .dropDuplicates([hub.hash_key])
            )
            self._ensure_table(hub_df, hub_table)
            hub_df.createOrReplaceTempView("_hub_updates")
            self.spark.sql(f"""
                MERGE INTO {hub_table} AS tgt
                USING _hub_updates AS src
                ON tgt.{hub.hash_key} = src.{hub.hash_key}
                WHEN NOT MATCHED THEN INSERT *
            """)
            inserted, updated = self._merge_metrics(hub_table)
            logger.info("Hub built: %s | inserted=%d", hub.name, inserted)
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=hub.name,
                    status="SUCCESS", start_time=start,
                    records_in=records_in, records_inserted=inserted, records_updated=updated,
                    source_table=self.config.flatten_table(topic), target_table=hub_table,
                )
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=hub.name,
                    status="FAILED", start_time=start, error_message=str(e),
                )
            logger.exception("Failed to build hub: %s → %s", hub.name, hub_table)
            raise

    # ------------------------------------------------------------------
    # Satellite
    # ------------------------------------------------------------------

    def build_satellite(self, topic: TopicConfig, sat: SatelliteConfig) -> None:
        hub: HubConfig = topic.rdv.hub
        sat_table = self.config.rdv_table(sat.table)
        logger.info("Building satellite: %s → %s", sat.name, sat_table)
        start = datetime.now(timezone.utc)
        try:
            df = self._flatten_df(topic)
            records_in = df.count() if self.audit_logger else None
            sat_df = df.select(
                F.md5(F.col(hub.business_key).cast("string")).alias(hub.hash_key),
                *[F.col(c) for c in sat.columns],
                F.col("event_timestamp").alias(LOAD_DATE_COL),
                F.md5(
                    F.concat_ws("||", *[F.col(c).cast("string") for c in sat.columns])
                ).alias(HASH_DIFF_COL),
                F.lit(RECORD_SOURCE).alias(RECORD_SOURCE_COL),
            )
            self._ensure_table(sat_df, sat_table)
            sat_df.createOrReplaceTempView("_sat_updates")
            self.spark.sql(f"""
                MERGE INTO {sat_table} AS tgt
                USING _sat_updates AS src
                ON tgt.{hub.hash_key} = src.{hub.hash_key}
                   AND tgt.{LOAD_DATE_COL} = src.{LOAD_DATE_COL}
                WHEN MATCHED AND tgt.{HASH_DIFF_COL} <> src.{HASH_DIFF_COL} THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            inserted, updated = self._merge_metrics(sat_table)
            logger.info("Satellite built: %s | inserted=%d updated=%d", sat.name, inserted, updated)
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=sat.name,
                    status="SUCCESS", start_time=start,
                    records_in=records_in, records_inserted=inserted, records_updated=updated,
                    source_table=self.config.flatten_table(topic), target_table=sat_table,
                )
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=sat.name,
                    status="FAILED", start_time=start, error_message=str(e),
                )
            logger.exception("Failed to build satellite: %s → %s", sat.name, sat_table)
            raise

    # ------------------------------------------------------------------
    # Link
    # ------------------------------------------------------------------

    def build_link(self, topic: TopicConfig, link: LinkConfig) -> None:
        hub: HubConfig = topic.rdv.hub
        link_table = self.config.rdv_table(link.table)
        logger.info("Building link: %s → %s", link.name, link_table)
        start = datetime.now(timezone.utc)
        try:
            all_biz_keys = [hub.business_key] + [ref.key for ref in link.foreign_refs]
            df = self._flatten_df(topic)
            records_in = df.count() if self.audit_logger else None
            link_df = (
                df.select(
                    F.md5(
                        F.concat_ws("||", *[F.col(k).cast("string") for k in all_biz_keys])
                    ).alias(link.hash_key),
                    F.md5(F.col(hub.business_key).cast("string")).alias(hub.hash_key),
                    *[
                        F.md5(F.col(ref.key).cast("string")).alias(ref.hash_key)
                        for ref in link.foreign_refs
                    ],
                    F.current_timestamp().alias(LOAD_DATE_COL),
                    F.lit(RECORD_SOURCE).alias(RECORD_SOURCE_COL),
                )
                .dropDuplicates([link.hash_key])
            )
            self._ensure_table(link_df, link_table)
            link_df.createOrReplaceTempView("_link_updates")
            self.spark.sql(f"""
                MERGE INTO {link_table} AS tgt
                USING _link_updates AS src
                ON tgt.{link.hash_key} = src.{link.hash_key}
                WHEN NOT MATCHED THEN INSERT *
            """)
            inserted, updated = self._merge_metrics(link_table)
            logger.info("Link built: %s | inserted=%d", link.name, inserted)
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=link.name,
                    status="SUCCESS", start_time=start,
                    records_in=records_in, records_inserted=inserted, records_updated=updated,
                    source_table=self.config.flatten_table(topic), target_table=link_table,
                )
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="rdv", topic=topic.name, entity=link.name,
                    status="FAILED", start_time=start, error_message=str(e),
                )
            logger.exception("Failed to build link: %s → %s", link.name, link_table)
            raise

    # ------------------------------------------------------------------
    # Process a full topic (hub + all satellites + all links)
    # ------------------------------------------------------------------

    def process_topic(self, topic: TopicConfig) -> None:
        logger.info("--- Processing RDV for topic: %s ---", topic.name)
        try:
            self.build_hub(topic)
            for sat in topic.rdv.satellites:
                self.build_satellite(topic, sat)
            for link in topic.rdv.links:
                self.build_link(topic, link)
            logger.info("RDV processing complete for topic: %s", topic.name)
        except Exception:
            logger.exception("RDV processing failed for topic: %s", topic.name)
            raise
