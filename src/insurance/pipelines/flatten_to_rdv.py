import sys

from insurance.processing.rdv_builder import RDVBuilder
from insurance.utils.audit import AuditLogger
from insurance.utils.config import MetadataConfig
from insurance.utils.dq_checker import DQChecker
from insurance.utils.logger import get_logger
from insurance.utils.spark_utils import get_spark

logger = get_logger(__name__)

_PIPELINE = "flatten_to_rdv"


def run(catalog: str, env: str, base_location: str) -> None:
    logger.info("=" * 60)
    logger.info("PIPELINE: %s | catalog=%s | env=%s", _PIPELINE, catalog, env)
    logger.info("=" * 60)
    try:
        config = MetadataConfig(catalog=catalog, env=env, base_location=base_location)
        spark = get_spark()
        audit = AuditLogger(spark=spark, catalog=catalog, env=env, pipeline=_PIPELINE)
        dq = DQChecker(spark=spark, config=config, audit_logger=audit)

        rdv_builder = RDVBuilder(spark=spark, config=config, audit_logger=audit)
        topics = config.get_pipeline_topics(_PIPELINE)
        logger.info("Topics to process: %s", [t.name for t in topics])

        for topic in topics:
            rdv_builder.process_topic(topic)
            dq.run_rdv_ri_checks(topic.name)

        logger.info("PIPELINE COMPLETE: %s", _PIPELINE)
    except Exception:
        logger.exception("PIPELINE FAILED: %s", _PIPELINE)
        sys.exit(1)
