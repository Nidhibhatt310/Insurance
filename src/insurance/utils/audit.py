import uuid
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from insurance.utils.logger import get_logger

logger = get_logger(__name__)

_PIPELINE_RUNS_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_secs", DoubleType(), True),
    StructField("records_in", LongType(), True),
    StructField("records_inserted", LongType(), True),
    StructField("records_updated", LongType(), True),
    StructField("records_skipped", LongType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("run_type", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("catalog", StringType(), True),
    StructField("env", StringType(), True),
    StructField("error_message", StringType(), True),
])

_DQ_CHECKS_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("check_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("threshold", StringType(), True),
    StructField("actual", StringType(), True),
    StructField("passed", BooleanType(), True),
    StructField("checked_at", TimestampType(), True),
])

_SCHEMA_CHANGES_SCHEMA = StructType([
    StructField("topic", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("detected_at", TimestampType(), True),
    StructField("change_type", StringType(), True),
    StructField("field_name", StringType(), True),
    StructField("old_type", StringType(), True),
    StructField("new_type", StringType(), True),
])


class AuditLogger:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        env: str,
        pipeline: str,
        run_type: str = "SCHEDULED",
    ):
        self.spark = spark
        self.catalog = catalog
        self.env = env
        self.pipeline = pipeline
        self.run_type = run_type
        self.run_id = str(uuid.uuid4())
        self._runs_table = f"{catalog}.audit.pipeline_runs"
        self._dq_table = f"{catalog}.audit.dq_checks"
        self._schema_table = f"{catalog}.audit.schema_changes"
        self._job_id = spark.conf.get("spark.databricks.clusterUsageTags.jobId", None)
        self._job_run_id = spark.conf.get("spark.databricks.clusterUsageTags.jobRunId", None)
        self._ensure_audit_tables()

    def _ensure_audit_tables(self) -> None:
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.audit")
            for table, schema in [
                (self._runs_table, _PIPELINE_RUNS_SCHEMA),
                (self._dq_table, _DQ_CHECKS_SCHEMA),
                (self._schema_table, _SCHEMA_CHANGES_SCHEMA),
            ]:
                self.spark.createDataFrame([], schema) \
                    .write.format("delta").mode("ignore").saveAsTable(table)
            logger.info("Audit tables ready: pipeline_runs, dq_checks, schema_changes")
        except Exception:
            logger.exception("Failed to initialise audit tables — audit logging may not work")

    def _write(self, table: str, row: dict, schema: StructType) -> None:
        try:
            self.spark.createDataFrame([row], schema) \
                .write.format("delta").mode("append").saveAsTable(table)
        except Exception:
            logger.exception("Failed to write audit record to %s", table)

    def log_run(
        self,
        layer: str,
        status: str,
        start_time: datetime,
        topic: Optional[str] = None,
        entity: Optional[str] = None,
        end_time: Optional[datetime] = None,
        records_in: Optional[int] = None,
        records_inserted: Optional[int] = None,
        records_updated: Optional[int] = None,
        source_table: Optional[str] = None,
        target_table: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        end = end_time or datetime.now(timezone.utc)
        duration = (end - start_time).total_seconds()

        records_skipped = None
        if records_in is not None:
            records_skipped = max(0, records_in - (records_inserted or 0) - (records_updated or 0))

        self._write(self._runs_table, {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "layer": layer,
            "topic": topic,
            "entity": entity,
            "status": status,
            "start_time": start_time,
            "end_time": end,
            "duration_secs": duration,
            "records_in": records_in,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_skipped": records_skipped,
            "source_table": source_table,
            "target_table": target_table,
            "run_type": self.run_type,
            "job_id": self._job_id,
            "job_run_id": self._job_run_id,
            "catalog": self.catalog,
            "env": self.env,
            "error_message": error_message[:2000] if error_message else None,
        }, _PIPELINE_RUNS_SCHEMA)

    def log_dq_check(
        self,
        layer: str,
        table_name: str,
        check_type: str,
        severity: str,
        threshold: float,
        actual: float,
        passed: bool,
        topic: Optional[str] = None,
        column_name: Optional[str] = None,
    ) -> None:
        self._write(self._dq_table, {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "layer": layer,
            "topic": topic,
            "table_name": table_name,
            "column_name": column_name,
            "check_type": check_type,
            "severity": severity,
            "threshold": str(threshold),
            "actual": str(round(actual, 6)),
            "passed": passed,
            "checked_at": datetime.now(timezone.utc),
        }, _DQ_CHECKS_SCHEMA)

    def log_schema_change(
        self,
        topic: str,
        change_type: str,
        field_name: str,
        old_type: Optional[str] = None,
        new_type: Optional[str] = None,
    ) -> None:
        self._write(self._schema_table, {
            "topic": topic,
            "pipeline": self.pipeline,
            "detected_at": datetime.now(timezone.utc),
            "change_type": change_type,
            "field_name": field_name,
            "old_type": old_type,
            "new_type": new_type,
        }, _SCHEMA_CHANGES_SCHEMA)
