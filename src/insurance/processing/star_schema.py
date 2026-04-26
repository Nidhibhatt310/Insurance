from datetime import datetime, timezone

from pyspark.sql import SparkSession

from insurance.utils.config import MetadataConfig
from insurance.utils.logger import get_logger

logger = get_logger(__name__)


class StarSchemaBuilder:
    """
    Generic gold-layer builder. Reads dimension and fact definitions from
    gold_schemas.yml and materialises SCD-Type-1 dimensions and fact tables
    by joining RDV hubs, satellites, and links.
    Adding a new gold table requires only a YAML change — no code changes needed.
    """

    def __init__(self, spark: SparkSession, config: MetadataConfig, audit_logger=None, dq_checker=None):
        self.spark = spark
        self.config = config
        self.audit_logger = audit_logger
        self.dq_checker = dq_checker

    def build_dimension(self, dim_cfg: dict) -> None:
        hub_table = self.config.rdv_table(dim_cfg["source_hub"])
        sat_table = self.config.rdv_table(dim_cfg["source_satellite"])
        dim_table = self.config.gold_table(dim_cfg["table"])
        hash_key = dim_cfg["hub_hash_key"]
        biz_key = dim_cfg["hub_biz_key"]
        cols = ", ".join(f"s.{c}" for c in dim_cfg["columns"])

        logger.info("Building dimension: %s → %s", dim_cfg["name"], dim_table)
        start = datetime.now(timezone.utc)
        try:
            self.spark.sql(f"""
                CREATE OR REPLACE TABLE {dim_table}
                USING DELTA AS
                SELECT
                    h.{hash_key},
                    h.{biz_key},
                    {cols},
                    s.load_date
                FROM {hub_table} h
                INNER JOIN (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {hash_key} ORDER BY load_date DESC) AS _rn
                    FROM {sat_table}
                ) s ON h.{hash_key} = s.{hash_key} AND s._rn = 1
            """)
            records_inserted = self.spark.table(dim_table).count()
            logger.info("Dimension built: %s | rows=%d", dim_cfg["name"], records_inserted)
            if self.dq_checker:
                self.dq_checker.run_gold_checks(dim_table)
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="gold", entity=dim_cfg["name"],
                    status="SUCCESS", start_time=start,
                    records_inserted=records_inserted,
                    source_table=f"{hub_table},{sat_table}",
                    target_table=dim_table,
                )
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="gold", entity=dim_cfg["name"],
                    status="FAILED", start_time=start, error_message=str(e),
                )
            logger.exception("Failed to build dimension: %s → %s", dim_cfg["name"], dim_table)
            raise

    def build_fact(self, fact_cfg: dict) -> None:
        hub_table = self.config.rdv_table(fact_cfg["source_hub"])
        sat_table = self.config.rdv_table(fact_cfg["source_satellite"])
        fact_table = self.config.gold_table(fact_cfg["table"])
        hash_key = fact_cfg["hub_hash_key"]
        biz_key = fact_cfg["hub_biz_key"]
        measures = ", ".join(f"s.{m}" for m in fact_cfg["measures"])

        dim_joins = ""
        dim_key_cols = ""
        for i, dim in enumerate(fact_cfg.get("dimensions", [])):
            alias = f"d{i}"
            link_tbl = self.config.rdv_table(dim["link_table"])
            dim_tbl = self.config.gold_table(dim["dim_table"])
            dim_hk = dim["dim_hash_key"]
            dim_joins += f"""
            LEFT JOIN {link_tbl} lnk{i}
                ON h.{hash_key} = lnk{i}.{hash_key}
            LEFT JOIN {dim_tbl} {alias}
                ON lnk{i}.{dim_hk} = {alias}.{dim_hk}"""
            dim_key_cols += f", {alias}.{dim_hk}"

        logger.info("Building fact: %s → %s", fact_cfg["name"], fact_table)
        start = datetime.now(timezone.utc)
        try:
            self.spark.sql(f"""
                CREATE OR REPLACE TABLE {fact_table}
                USING DELTA AS
                SELECT
                    h.{hash_key},
                    h.{biz_key},
                    {measures}{dim_key_cols},
                    s.load_date
                FROM {hub_table} h
                INNER JOIN (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {hash_key} ORDER BY load_date DESC) AS _rn
                    FROM {sat_table}
                ) s ON h.{hash_key} = s.{hash_key} AND s._rn = 1
                {dim_joins}
            """)
            records_inserted = self.spark.table(fact_table).count()
            logger.info("Fact built: %s | rows=%d", fact_cfg["name"], records_inserted)
            if self.dq_checker:
                self.dq_checker.run_gold_checks(fact_table)
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="gold", entity=fact_cfg["name"],
                    status="SUCCESS", start_time=start,
                    records_inserted=records_inserted,
                    source_table=f"{hub_table},{sat_table}",
                    target_table=fact_table,
                )
        except Exception as e:
            if self.audit_logger:
                self.audit_logger.log_run(
                    layer="gold", entity=fact_cfg["name"],
                    status="FAILED", start_time=start, error_message=str(e),
                )
            logger.exception("Failed to build fact: %s → %s", fact_cfg["name"], fact_table)
            raise
