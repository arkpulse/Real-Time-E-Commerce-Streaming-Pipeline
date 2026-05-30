"""
Data Quality Framework
========================
Reusable DQ validation engine for all pipeline layers.
Generates quality reports, tracks metrics, and publishes alerts.

Author: Data Engineering Team
Version: 1.0.0
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger("DataQuality")


class Severity(str, Enum):
    CRITICAL = "CRITICAL"   # Pipeline should halt
    HIGH     = "HIGH"       # Alert and log, continue
    MEDIUM   = "MEDIUM"     # Log warning, continue
    LOW      = "LOW"        # Informational


class CheckStatus(str, Enum):
    PASS    = "PASS"
    FAIL    = "FAIL"
    WARNING = "WARNING"
    SKIPPED = "SKIPPED"


@dataclass
class DQCheck:
    name: str
    description: str
    severity: Severity
    check_fn: Callable[[Dict], Tuple[bool, Optional[str]]]
    category: str = "generic"

    def run(self, record: Dict) -> "DQResult":
        try:
            passed, message = self.check_fn(record)
            status = CheckStatus.PASS if passed else CheckStatus.FAIL
            return DQResult(
                check_name=self.name,
                status=status,
                severity=self.severity,
                message=message or ("OK" if passed else "Check failed"),
                category=self.category,
            )
        except Exception as e:
            return DQResult(
                check_name=self.name,
                status=CheckStatus.FAIL,
                severity=Severity.HIGH,
                message=f"Check raised exception: {e}",
                category=self.category,
            )


@dataclass
class DQResult:
    check_name: str
    status: CheckStatus
    severity: Severity
    message: str
    category: str = "generic"
    evaluated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def passed(self) -> bool:
        return self.status == CheckStatus.PASS


@dataclass
class DQReport:
    entity: str
    record_id: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    results: List[DQResult]
    overall_status: str
    quality_score: float
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["results"] = [asdict(r) for r in self.results]
        return d

    def has_critical_failures(self) -> bool:
        return any(
            r.severity == Severity.CRITICAL and r.status == CheckStatus.FAIL
            for r in self.results
        )


# ─────────────────────────────────────────────
# Built-in Check Library
# ─────────────────────────────────────────────

class CommonChecks:
    """Standard DQ checks reusable across entities."""

    @staticmethod
    def not_null(field_name: str, severity: Severity = Severity.HIGH) -> DQCheck:
        return DQCheck(
            name=f"not_null.{field_name}",
            description=f"Field '{field_name}' must not be null or empty",
            severity=severity,
            category="completeness",
            check_fn=lambda r: (
                r.get(field_name) is not None and r.get(field_name) != "",
                f"'{field_name}' is null/empty" if r.get(field_name) is None else None,
            ),
        )

    @staticmethod
    def positive_number(field_name: str, severity: Severity = Severity.HIGH) -> DQCheck:
        return DQCheck(
            name=f"positive.{field_name}",
            description=f"Field '{field_name}' must be > 0",
            severity=severity,
            category="validity",
            check_fn=lambda r: (
                isinstance(r.get(field_name), (int, float)) and r.get(field_name, 0) > 0,
                f"'{field_name}' = {r.get(field_name)} is not positive",
            ),
        )

    @staticmethod
    def max_value(field_name: str, max_val: float, severity: Severity = Severity.MEDIUM) -> DQCheck:
        return DQCheck(
            name=f"max_value.{field_name}",
            description=f"Field '{field_name}' must be ≤ {max_val}",
            severity=severity,
            category="validity",
            check_fn=lambda r: (
                r.get(field_name, 0) <= max_val,
                f"'{field_name}' = {r.get(field_name)} exceeds max {max_val}",
            ),
        )

    @staticmethod
    def valid_enum(field_name: str, allowed: set, severity: Severity = Severity.MEDIUM) -> DQCheck:
        return DQCheck(
            name=f"valid_enum.{field_name}",
            description=f"Field '{field_name}' must be one of {allowed}",
            severity=severity,
            category="validity",
            check_fn=lambda r: (
                r.get(field_name) in allowed,
                f"'{field_name}' = '{r.get(field_name)}' not in allowed values",
            ),
        )

    @staticmethod
    def valid_timestamp(field_name: str) -> DQCheck:
        def _check(r):
            ts = r.get(field_name)
            if not ts:
                return False, f"'{field_name}' is missing"
            try:
                datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                return True, None
            except ValueError:
                return False, f"'{field_name}' = '{ts}' is not valid ISO timestamp"
        return DQCheck(
            name=f"valid_timestamp.{field_name}",
            description=f"Field '{field_name}' must be a valid ISO timestamp",
            severity=Severity.HIGH,
            category="validity",
            check_fn=_check,
        )

    @staticmethod
    def string_format(field_name: str, prefix: str) -> DQCheck:
        return DQCheck(
            name=f"format.{field_name}",
            description=f"Field '{field_name}' must start with '{prefix}'",
            severity=Severity.MEDIUM,
            category="consistency",
            check_fn=lambda r: (
                str(r.get(field_name, "")).startswith(prefix),
                f"'{field_name}' = '{r.get(field_name)}' does not start with '{prefix}'",
            ),
        )

    @staticmethod
    def no_future_timestamp(field_name: str, tolerance_seconds: int = 60) -> DQCheck:
        def _check(r):
            ts_str = r.get(field_name)
            if not ts_str:
                return True, None  # handled by not_null check
            try:
                ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                diff = (ts - now).total_seconds()
                return diff <= tolerance_seconds, f"'{field_name}' is {diff:.0f}s in the future"
            except ValueError:
                return True, None
        return DQCheck(
            name=f"no_future.{field_name}",
            description=f"'{field_name}' must not be a future timestamp",
            severity=Severity.MEDIUM,
            category="timeliness",
            check_fn=_check,
        )


# ─────────────────────────────────────────────
# Entity-Specific Check Sets
# ─────────────────────────────────────────────

ORDER_CHECKS: List[DQCheck] = [
    CommonChecks.not_null("event_id",         Severity.CRITICAL),
    CommonChecks.not_null("order_id",         Severity.CRITICAL),
    CommonChecks.not_null("user_id",          Severity.CRITICAL),
    CommonChecks.not_null("session_id",       Severity.HIGH),
    CommonChecks.not_null("total_amount",     Severity.CRITICAL),
    CommonChecks.not_null("currency",         Severity.HIGH),
    CommonChecks.positive_number("total_amount", Severity.CRITICAL),
    CommonChecks.max_value("total_amount", 50000, Severity.HIGH),
    CommonChecks.valid_enum("currency", {"USD","EUR","GBP","CAD","AUD","JPY"}, Severity.MEDIUM),
    CommonChecks.valid_enum("order_status", {"pending","confirmed","processing","shipped","delivered","cancelled","refunded"}, Severity.MEDIUM),
    CommonChecks.valid_timestamp("event_timestamp"),
    CommonChecks.no_future_timestamp("event_timestamp"),
    CommonChecks.string_format("order_id", "ORD-"),
    CommonChecks.string_format("user_id",  "USR-"),
    DQCheck(
        name="order_items_not_empty",
        description="Order must have at least one item",
        severity=Severity.CRITICAL,
        category="completeness",
        check_fn=lambda r: (isinstance(r.get("items"), list) and len(r.get("items", [])) > 0,
                            "Order has no items"),
    ),
    DQCheck(
        name="order_items_max",
        description="Order should not have more than 100 items",
        severity=Severity.MEDIUM,
        category="validity",
        check_fn=lambda r: (len(r.get("items", [])) <= 100,
                            f"Order has {len(r.get('items', []))} items (>100)"),
    ),
]

PAYMENT_CHECKS: List[DQCheck] = [
    CommonChecks.not_null("payment_id",   Severity.CRITICAL),
    CommonChecks.not_null("order_id",     Severity.CRITICAL),
    CommonChecks.not_null("amount",       Severity.CRITICAL),
    CommonChecks.positive_number("amount", Severity.CRITICAL),
    CommonChecks.max_value("amount", 100000, Severity.HIGH),
    CommonChecks.valid_enum("payment_status", {"success","failed","declined","pending","refunded"}, Severity.HIGH),
    CommonChecks.valid_timestamp("event_timestamp"),
    CommonChecks.string_format("payment_id", "PAY-"),
]

CLICKSTREAM_CHECKS: List[DQCheck] = [
    CommonChecks.not_null("event_id",    Severity.HIGH),
    CommonChecks.not_null("session_id",  Severity.HIGH),
    CommonChecks.valid_enum("page_type", {"home","category","product_detail","search_results","cart","checkout","order_confirmation","account","wishlist"}, Severity.LOW),
    CommonChecks.valid_timestamp("event_timestamp"),
    DQCheck(
        name="time_on_page_valid",
        description="Time on page should be between 0 and 3600 seconds",
        severity=Severity.LOW,
        category="validity",
        check_fn=lambda r: (0 <= r.get("time_on_page_seconds", 0) <= 3600,
                            f"time_on_page_seconds = {r.get('time_on_page_seconds')}"),
    ),
]


# ─────────────────────────────────────────────
# DQ Engine
# ─────────────────────────────────────────────

class DataQualityEngine:
    """
    Runs registered checks against records and produces DQReports.
    Publishes metrics to CloudWatch and alerts via SNS.
    """

    CHECK_SETS = {
        "orders":      ORDER_CHECKS,
        "payments":    PAYMENT_CHECKS,
        "clickstream": CLICKSTREAM_CHECKS,
    }

    def __init__(self, entity: str, sns_topic_arn: str = ""):
        if entity not in self.CHECK_SETS:
            raise ValueError(f"Unknown entity: {entity}. Choose from {list(self.CHECK_SETS.keys())}")
        self.entity = entity
        self.checks = self.CHECK_SETS[entity]
        self.sns_topic_arn = sns_topic_arn
        self._cloudwatch = boto3.client("cloudwatch") if os.getenv("AWS_REGION") else None
        self._sns = boto3.client("sns") if sns_topic_arn else None

    def validate(self, record: Dict) -> DQReport:
        """Run all checks for this entity against a single record."""
        results = [check.run(record) for check in self.checks]

        passed   = sum(1 for r in results if r.passed)
        failed   = sum(1 for r in results if not r.passed)
        warnings = sum(1 for r in results
                       if not r.passed and r.severity in (Severity.LOW, Severity.MEDIUM))

        total = len(results)
        quality_score = round(passed / total, 4) if total > 0 else 0.0
        overall_status = (
            "PASS"    if failed == 0
            else "CRITICAL" if any(r.severity == Severity.CRITICAL and not r.passed for r in results)
            else "FAIL"
        )

        record_id = (
            record.get("order_id") or record.get("payment_id") or
            record.get("event_id") or "unknown"
        )

        return DQReport(
            entity=self.entity,
            record_id=str(record_id),
            total_checks=total,
            passed=passed,
            failed=failed,
            warnings=warnings,
            results=results,
            overall_status=overall_status,
            quality_score=quality_score,
        )

    def validate_batch(self, records: List[Dict]) -> Dict[str, Any]:
        """
        Validate a batch of records. Returns summary statistics.
        """
        reports        = [self.validate(r) for r in records]
        total          = len(reports)
        passed_reports = sum(1 for r in reports if r.overall_status == "PASS")
        failed_reports = total - passed_reports
        critical       = sum(1 for r in reports if r.has_critical_failures())
        avg_score      = round(sum(r.quality_score for r in reports) / total, 4) if total > 0 else 0

        # Per-check failure summary
        check_failures: Dict[str, int] = {}
        for report in reports:
            for result in report.results:
                if not result.passed:
                    check_failures[result.check_name] = check_failures.get(result.check_name, 0) + 1

        summary = {
            "entity": self.entity,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
            "total_records": total,
            "passed_records": passed_reports,
            "failed_records": failed_reports,
            "critical_failures": critical,
            "pass_rate_pct": round(passed_reports / total * 100, 2) if total > 0 else 0,
            "avg_quality_score": avg_score,
            "check_failure_counts": check_failures,
            "valid_records": [r.record_id for r in reports if r.overall_status == "PASS"],
            "invalid_records": [r.record_id for r in reports if r.overall_status != "PASS"],
        }

        # Publish CW metrics
        self._publish_cloudwatch_metrics(summary)

        # Alert if failure rate > 10%
        fail_rate = failed_reports / total if total > 0 else 0
        if fail_rate > 0.10:
            self._send_alert(
                f"High DQ failure rate: {self.entity}",
                f"Failure rate: {fail_rate:.1%} ({failed_reports}/{total})\n"
                f"Top failures: {sorted(check_failures.items(), key=lambda x: -x[1])[:5]}"
            )

        return summary

    def _publish_cloudwatch_metrics(self, summary: Dict) -> None:
        if not self._cloudwatch:
            return
        try:
            self._cloudwatch.put_metric_data(
                Namespace="EcommerceDataQuality",
                MetricData=[
                    {"MetricName": "PassRate", "Value": summary["pass_rate_pct"],
                     "Unit": "Percent", "Dimensions": [{"Name": "Entity", "Value": self.entity}]},
                    {"MetricName": "CriticalFailures", "Value": summary["critical_failures"],
                     "Unit": "Count", "Dimensions": [{"Name": "Entity", "Value": self.entity}]},
                    {"MetricName": "AvgQualityScore", "Value": summary["avg_quality_score"],
                     "Unit": "None", "Dimensions": [{"Name": "Entity", "Value": self.entity}]},
                ]
            )
        except Exception as e:
            logger.warning(f"CloudWatch metrics publish failed: {e}")

    def _send_alert(self, subject: str, message: str) -> None:
        if not self._sns or not self.sns_topic_arn:
            return
        try:
            self._sns.publish(
                TopicArn=self.sns_topic_arn,
                Subject=f"[DQ ALERT] {subject}",
                Message=message,
            )
        except Exception as e:
            logger.warning(f"SNS alert failed: {e}")
