"""
AWS Lambda: Orders Stream Processor
=====================================
Processes order events from Kinesis stream.
Validates schema, enriches records, performs DQ checks,
and persists to S3 bronze layer in Parquet format.

Author: Data Engineering Team
Runtime: Python 3.11
Memory: 512 MB
Timeout: 300s
"""

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
dynamodb = boto3.resource("dynamodb")

S3_BUCKET = os.environ["S3_BUCKET"]
DLQ_STREAM = os.environ.get("DLQ_STREAM", "ecommerce-dlq-stream")
SNS_ALERT_ARN = os.environ.get("SNS_ALERT_ARN", "")
DEDUP_TABLE = os.environ.get("DEDUP_TABLE", "ecommerce-dedup-orders")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")


# ─────────────────────────────────────────────
# Schema Definition
# ─────────────────────────────────────────────

REQUIRED_FIELDS = {
    "event_id", "event_type", "event_timestamp", "session_id",
    "user_id", "order_id", "total_amount", "items", "payment_method",
    "currency", "country",
}

NUMERIC_FIELDS = {"total_amount", "subtotal", "tax_amount", "shipping_cost", "item_count"}
STRING_FIELDS = {"event_id", "event_type", "user_id", "session_id", "order_id", "currency", "country"}


# ─────────────────────────────────────────────
# Data Quality Checks
# ─────────────────────────────────────────────

class DataQualityError(Exception):
    def __init__(self, reason: str, record: Dict):
        self.reason = reason
        self.record = record
        super().__init__(reason)


def validate_schema(record: Dict) -> Tuple[bool, Optional[str]]:
    """Validate that all required fields are present and non-null."""
    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        return False, f"Missing required fields: {missing}"

    null_required = [f for f in REQUIRED_FIELDS if record.get(f) is None]
    if null_required:
        return False, f"Null values in required fields: {null_required}"

    return True, None


def validate_business_rules(record: Dict) -> Tuple[bool, Optional[str]]:
    """Apply business logic validation rules."""
    # Amount validations
    total = record.get("total_amount", 0)
    if not isinstance(total, (int, float)) or total <= 0:
        return False, f"Invalid total_amount: {total}"
    if total > 50000:
        return False, f"Suspiciously high total_amount: {total}"

    # Items validation
    items = record.get("items", [])
    if not isinstance(items, list) or len(items) == 0:
        return False, "Order has no items"
    if len(items) > 100:
        return False, f"Unrealistic item count: {len(items)}"

    # Currency validation
    if record.get("currency") not in {"USD", "EUR", "GBP", "CAD", "AUD", "JPY"}:
        return False, f"Unsupported currency: {record.get('currency')}"

    # Timestamp validation
    ts = record.get("event_timestamp", "")
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, f"Invalid event_timestamp: {ts}"

    return True, None


def check_duplicate(order_id: str, table_name: str) -> bool:
    """
    Check for duplicate order IDs using DynamoDB as dedup store.
    Returns True if duplicate detected.
    """
    try:
        table = dynamodb.Table(table_name)
        response = table.get_item(
            Key={"order_id": order_id},
            ConsistentRead=True,
        )
        if "Item" in response:
            logger.warning(f"Duplicate order detected: {order_id}")
            return True

        # Store with 24-hour TTL
        ttl = int(datetime.now(timezone.utc).timestamp()) + 86400
        table.put_item(Item={"order_id": order_id, "processed_at": datetime.now(timezone.utc).isoformat(), "ttl": ttl})
        return False

    except ClientError as e:
        logger.error(f"DynamoDB error during dedup check: {e}")
        return False  # Fail open to avoid blocking pipeline


# ─────────────────────────────────────────────
# Record Enrichment
# ─────────────────────────────────────────────

def enrich_record(record: Dict) -> Dict:
    """Add computed and metadata fields to the record."""
    now = datetime.now(timezone.utc).isoformat()
    record["ingestion_timestamp"] = now
    record["processing_id"] = f"PROC-{uuid.uuid4().hex[:8].upper()}"
    record["environment"] = ENVIRONMENT

    # Derived metrics
    items = record.get("items", [])
    record["item_count"] = len(items)
    record["average_item_value"] = round(
        record.get("subtotal", 0) / len(items), 2
    ) if items else 0

    # Temporal partitioning fields
    try:
        ts = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
        record["year"] = ts.year
        record["month"] = ts.month
        record["day"] = ts.day
        record["hour"] = ts.hour
        record["day_of_week"] = ts.strftime("%A")
        record["is_weekend"] = ts.weekday() >= 5
    except Exception:
        now_dt = datetime.now(timezone.utc)
        record["year"] = now_dt.year
        record["month"] = now_dt.month
        record["day"] = now_dt.day
        record["hour"] = now_dt.hour

    return record


# ─────────────────────────────────────────────
# S3 Writer
# ─────────────────────────────────────────────

def write_to_s3_bronze(records: List[Dict], batch_id: str) -> str:
    """
    Write validated records to S3 Bronze zone as newline-delimited JSON (NDJSON).
    Partitioned by year/month/day/hour for efficient querying.
    """
    if not records:
        return ""

    now = datetime.now(timezone.utc)
    partition_path = (
        f"bronze/orders/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
    )
    key = f"{partition_path}orders_{batch_id}_{now.strftime('%H%M%S')}.json"

    payload = "\n".join(json.dumps(r, default=str) for r in records)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/x-ndjson",
            Metadata={
                "record_count": str(len(records)),
                "batch_id": batch_id,
                "environment": ENVIRONMENT,
            },
        )
        logger.info(f"Written {len(records)} records to s3://{S3_BUCKET}/{key}")
        return f"s3://{S3_BUCKET}/{key}"
    except ClientError as e:
        logger.error(f"S3 write failed: {e}")
        raise


def send_to_dlq(record: Dict, reason: str) -> None:
    """Route failed records to the Dead Letter Queue (DLQ) S3 path."""
    now = datetime.now(timezone.utc)
    dlq_record = {
        "original_record": record,
        "failure_reason": reason,
        "failed_at": now.isoformat(),
        "stream": "ecommerce-orders-stream",
        "environment": ENVIRONMENT,
    }

    key = (
        f"dlq/orders/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"failed_{uuid.uuid4().hex[:8]}.json"
    )

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(dlq_record, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.warning(f"DLQ record written: s3://{S3_BUCKET}/{key} | reason={reason}")
    except ClientError as e:
        logger.error(f"Failed to write DLQ record: {e}")


def send_sns_alert(subject: str, message: str) -> None:
    """Send operational alert via AWS SNS."""
    if not SNS_ALERT_ARN:
        return
    try:
        sns_client.publish(
            TopicArn=SNS_ALERT_ARN,
            Subject=f"[{ENVIRONMENT.upper()}] {subject}",
            Message=message,
        )
    except ClientError as e:
        logger.error(f"SNS alert failed: {e}")


# ─────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Main Lambda entry point for Kinesis trigger.
    Processes batches of Kinesis records with full DQ pipeline.
    """
    records = event.get("Records", [])
    batch_id = context.aws_request_id if context else uuid.uuid4().hex

    logger.info(f"Processing batch | batch_id={batch_id} | record_count={len(records)}")

    valid_records = []
    failed_records = []
    duplicate_count = 0

    for kinesis_record in records:
        raw_data = None
        try:
            # Decode base64-encoded Kinesis payload
            raw_bytes = base64.b64decode(kinesis_record["kinesis"]["data"])
            raw_data = json.loads(raw_bytes.decode("utf-8"))

            # 1. Schema validation
            is_valid, schema_error = validate_schema(raw_data)
            if not is_valid:
                failed_records.append((raw_data, f"Schema error: {schema_error}"))
                continue

            # 2. Business rule validation
            is_valid, biz_error = validate_business_rules(raw_data)
            if not is_valid:
                failed_records.append((raw_data, f"Business rule: {biz_error}"))
                continue

            # 3. Deduplication check
            order_id = raw_data["order_id"]
            if check_duplicate(order_id, DEDUP_TABLE):
                duplicate_count += 1
                continue

            # 4. Enrich record
            enriched = enrich_record(raw_data)
            valid_records.append(enriched)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            failed_records.append(({}, f"JSON decode error: {e}"))
        except Exception as e:
            logger.exception(f"Unexpected error processing record: {e}")
            failed_records.append((raw_data or {}, f"Processing error: {e}"))

    # Write valid records to S3 bronze
    s3_path = ""
    if valid_records:
        try:
            s3_path = write_to_s3_bronze(valid_records, batch_id)
        except Exception as e:
            logger.error(f"S3 write failed for batch {batch_id}: {e}")
            send_sns_alert(
                "Orders Lambda: S3 Write Failure",
                f"Batch {batch_id} failed to write {len(valid_records)} records.\nError: {str(e)}"
            )

    # Route failed records to DLQ
    for failed_record, reason in failed_records:
        send_to_dlq(failed_record, reason)

    # Alert if failure rate exceeds threshold
    total = len(records)
    failure_rate = len(failed_records) / total if total > 0 else 0
    if failure_rate > 0.10 and total >= 10:  # Alert if >10% failure rate
        send_sns_alert(
            "Orders Lambda: High Failure Rate",
            f"Batch {batch_id}: {failure_rate:.1%} failure rate ({len(failed_records)}/{total})"
        )

    result = {
        "statusCode": 200,
        "batch_id": batch_id,
        "total_records": total,
        "valid_records": len(valid_records),
        "failed_records": len(failed_records),
        "duplicate_records": duplicate_count,
        "failure_rate_pct": round(failure_rate * 100, 2),
        "s3_path": s3_path,
    }

    logger.info(f"Batch complete | {json.dumps(result)}")
    return result
