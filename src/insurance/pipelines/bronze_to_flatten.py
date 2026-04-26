import sys
from datetime import datetime, timezone

from insurance.processing.standardize_data import BronzeFlattener
from insurance.utils.audit import AuditLogger
from insurance.utils.config import MetadataConfig
from insurance.utils.dq_checker import DQChecker
from insurance.utils.logger import get_logger
from insurance.utils.spark_utils import get_spark

logger = get_logger(__name__)

_PIPELINE = "bronze_to_flatten"


def run(catalog: str, env: str, base_location: str) -> None:
    logger.info("=" * 60)
    logger.info("PIPELINE: %s | catalog=%s | env=%s", _PIPELINE, catalog, env)
    logger.info("=" * 60)
    try:
        config = MetadataConfig(catalog=catalog, env=env, base_location=base_location)
        spark = get_spark()
        audit = AuditLogger(spark=spark, catalog=catalog, env=env, pipeline=_PIPELINE)
        dq = DQChecker(spark=spark, config=config, audit_logger=audit)

        flattener = BronzeFlattener(spark=spark, config=config)
        topics = config.get_pipeline_topics(_PIPELINE)
        logger.info("Topics to flatten: %s", [t.name for t in topics])

        # Start all streams in parallel, then await + DQ + audit each
        started = []
        for topic in topics:
            start = datetime.now(timezone.utc)
            df = flattener.flatten_topic(topic, audit_logger=audit)
            q = flattener.write_flatten(df, topic)
            started.append((topic, q, start))

        for topic, q, start in started:
            try:
                q.awaitTermination()
                progress = q.lastProgress or {}
                records_in = int(progress.get("numInputRows", 0))
                dq.run_topic_checks(config.flatten_table(topic), "flatten", topic.name)
                audit.log_run(
                    layer="flatten",
                    topic=topic.name,
                    status="SUCCESS",
                    start_time=start,
                    records_in=records_in,
                    records_inserted=records_in,
                    source_table=config.bronze_table(topic),
                    target_table=config.flatten_table(topic),
                )
            except Exception as e:
                audit.log_run(
                    layer="flatten",
                    topic=topic.name,
                    status="FAILED",
                    start_time=start,
                    error_message=str(e),
                )
                raise

        logger.info("PIPELINE COMPLETE: %s", _PIPELINE)
    except Exception:
        logger.exception("PIPELINE FAILED: %s", _PIPELINE)
        sys.exit(1)
