from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from insurance.utils.logger import get_logger

logger = get_logger(__name__)


class DQChecker:
    """
    Metadata-driven data quality checker. Rules are loaded from dq_rules.yml via
    MetadataConfig and evaluated against Delta tables after each layer write.
    Adding a new check for any topic requires only a YAML change — no code changes needed.
    """

    def __init__(self, spark: SparkSession, config, audit_logger):
        self.spark = spark
        self.catalog = config.catalog
        self.audit_logger = audit_logger
        self._rules = config.get_dq_rules()

    # ------------------------------------------------------------------
    # Public — single-table checks (flatten + gold)
    # ------------------------------------------------------------------

    def run_topic_checks(self, table_name: str, layer: str, topic_name: str) -> bool:
        """Read target table and run all single-table checks for a topic+layer.
        Returns False if any CRITICAL check fails."""
        rules = [
            r for r in self._rules.get("topics", {}).get(topic_name, {}).get(layer, [])
            if r.get("check") != "referential_integrity"
        ]
        if not rules:
            return True
        try:
            df = self.spark.table(table_name)
            return self._evaluate_rules(df, rules, layer, topic_name, table_name)
        except Exception:
            logger.exception("DQ: failed to read %s for checks — skipping", table_name)
            return True

    def run_gold_checks(self, table_name: str) -> bool:
        """Run the global gold-layer checks (row_count_min etc.) against a gold table."""
        rules = self._rules.get("gold", [])
        if not rules:
            return True
        try:
            df = self.spark.table(table_name)
            return self._evaluate_rules(df, rules, "gold", None, table_name)
        except Exception:
            logger.exception("DQ: failed to read %s for gold checks — skipping", table_name)
            return True

    # ------------------------------------------------------------------
    # Public — referential integrity checks (rdv layer)
    # ------------------------------------------------------------------

    def run_rdv_ri_checks(self, topic_name: str) -> bool:
        """Run referential integrity checks defined under topics.<name>.rdv in dq_rules.yml.
        Each rule verifies that every foreign hash key in a link table exists in its hub.
        Returns False if any CRITICAL check fails."""
        rules = [
            r for r in self._rules.get("topics", {}).get(topic_name, {}).get("rdv", [])
            if r.get("check") == "referential_integrity"
        ]
        if not rules:
            return True

        all_critical_pass = True
        for rule in rules:
            link_table = f"{self.catalog}.{rule['link_table']}"
            ref_table  = f"{self.catalog}.{rule['ref_table']}"
            column     = rule["column"]
            ref_column = rule["ref_column"]
            threshold  = float(rule.get("threshold", 0.0))
            severity   = rule.get("severity", "WARNING")

            passed, actual = self._run_ri_check(link_table, ref_table, column, ref_column, threshold)

            self.audit_logger.log_dq_check(
                layer="rdv",
                topic=topic_name,
                table_name=link_table,
                column_name=column,
                check_type="referential_integrity",
                severity=severity,
                threshold=threshold,
                actual=actual,
                passed=passed,
            )

            result = "PASS" if passed else "FAIL"
            logger.info(
                "DQ RI [%s] %s.%s → %s | orphan_rate=%.6f threshold=%s → %s",
                severity, link_table, column, ref_table, actual, threshold, result,
            )
            if not passed and severity == "CRITICAL":
                logger.error(
                    "CRITICAL RI check failed: %s.%s has %.2f%% orphaned keys vs %s",
                    link_table, column, actual * 100, ref_table,
                )
                all_critical_pass = False

        return all_critical_pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _evaluate_rules(
        self,
        df: DataFrame,
        rules: list,
        layer: str,
        topic_name: Optional[str],
        table_name: str,
    ) -> bool:
        try:
            total = df.count()
        except Exception:
            logger.exception("DQ: could not count rows in %s — skipping", table_name)
            return True

        all_critical_pass = True
        for rule in rules:
            check_type = rule["check"]
            column     = rule.get("column")
            threshold  = float(rule.get("threshold", 0))
            severity   = rule.get("severity", "WARNING")

            passed, actual = self._run_check(df, total, check_type, column, threshold)

            self.audit_logger.log_dq_check(
                layer=layer,
                topic=topic_name,
                table_name=table_name,
                column_name=column,
                check_type=check_type,
                severity=severity,
                threshold=threshold,
                actual=actual,
                passed=passed,
            )

            result = "PASS" if passed else "FAIL"
            logger.info(
                "DQ [%s] %s | %s on %s | threshold=%s actual=%.6f → %s",
                severity, table_name, check_type, column or "*", threshold, actual, result,
            )
            if not passed and severity == "CRITICAL":
                logger.error("CRITICAL DQ check failed: %s | %s on %s", table_name, check_type, column)
                all_critical_pass = False

        return all_critical_pass

    def _run_check(
        self,
        df: DataFrame,
        total: int,
        check_type: str,
        column: Optional[str],
        threshold: float,
    ) -> tuple:
        if total == 0:
            return True, 0.0
        try:
            if check_type == "null_rate":
                null_count = df.filter(F.col(column).isNull()).count()
                actual = null_count / total
                return actual <= threshold, actual

            if check_type == "row_count_min":
                return total >= int(threshold), float(total)

            if check_type == "row_count_max":
                return total <= int(threshold), float(total)

            logger.warning("DQ: unknown check type '%s' — skipping", check_type)
            return True, 0.0
        except Exception:
            logger.exception("DQ: error running %s on column %s", check_type, column)
            return True, 0.0

    def _run_ri_check(
        self,
        link_table: str,
        ref_table: str,
        column: str,
        ref_column: str,
        threshold: float,
    ) -> tuple:
        """Left-anti join link_table against ref_table to find orphaned foreign keys.
        Returns (passed, orphan_rate) where passed = orphan_rate <= threshold."""
        try:
            link_df = self.spark.table(link_table)
            ref_df  = self.spark.table(ref_table)
            total   = link_df.count()
            if total == 0:
                return True, 0.0
            orphans = (
                link_df.join(ref_df, link_df[column] == ref_df[ref_column], "left_anti")
                .count()
            )
            orphan_rate = orphans / total
            return orphan_rate <= threshold, orphan_rate
        except Exception:
            logger.exception("DQ RI: check failed — %s.%s → %s", link_table, column, ref_table)
            return True, 0.0
