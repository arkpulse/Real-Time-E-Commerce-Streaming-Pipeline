"""
AWS Lambda: Payments Stream Processor
=======================================
Processes payment events from Kinesis stream.
Validates schema, detects fraud signals, enriches records,
and persists to S3 bronze layer.

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

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")
dynamodb   = boto3.resource("dynamodb")

S3_BUCKET     = os.environ["S3_BUCKET"]
SNS_ALERT_ARN = os.environ.get("SNS_ALERT_ARN", "")
DEDUP_TABLE   = os.environ.get("DEDUP_TABLE", "ecommerce-dedup-payments")
ENVIRONMENT   = os.environ.get("ENVIRONMENT", "prod")

REQUIRED_FIELDS = {
    "event_id", "payment_id", "order_id", "user_id",
    "amount", "currency", "payment_method", "payment_status",
    "event_timestamp",
}

VALID_STATUSES = {"success", "failed", "declined", "pending", "refunded"}
VALID_METHODS  = {"credit_card", "debit_card", "paypal", "apple_pay",
                  "google_pay", "bank_transfer", "crypto"}


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────

def validate_schema(record: Dict) -> Tuple[bool, Optional[str]]:
    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        return False, f"Missing fields: {missing}"
    nulls = [f for f in REQUIRED_FIELDS if record.get(f) is None]
    if nulls:
        return False, f"Null required fields: {nulls}"
    return True, None


def validate_payment(record: Dict) -> Tuple[bool, Optional[str]]:
    amount = record.get("amount", 0)
    if not isinstance(amount, (int, float)) or amount <= 0:
        return False, f"Invalid amount: {amount}"
    if amount > 100000:
        return False, f"Amount exceeds limit: {amount}"
    if record.get("payment_status") not in VALID_STATUSES:
        return False, f"Invalid status: {record.get('payment_status')}"
    try:
        datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, f"Invalid timestamp: {record.get('event_timestamp')}"
    return True, None


def detect_fraud_signals(record: Dict) -> List[str]:
    """Return a list of fraud signal strings (non-blocking — just flagged)."""
    signals = []
    amount = record.get("amount", 0)
    if amount > 5000:
        signals.append("high_value_transaction")
    if record.get("failure_reason") == "fraud_detected":
        signals.append("gateway_fraud_flag")
    if record.get("is_3ds_authenticated") is False and amount > 500:
        signals.append("no_3ds_high_value")
    if record.get("gateway_response_code") in ("59", "62", "63"):
        signals.append("suspicious_response_code")
    return signals


def check_duplicate(payment_id: str) -> bool:
    try:
        table = dynamodb.Table(DEDUP_TABLE)
        response = table.get_item(Key={"payment_id": payment_id}, ConsistentRead=True)
        if "Item" in response:
            return True
        ttl = int(datetime.now(timezone.utc).timestamp()) + 86400
        table.put_item(Item={
            "payment_id": payment_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "ttl": ttl,
        })
        return False
    except ClientError as e:
        logger.error(f"DynamoDB dedup error: {e}")
        return False


def enrich_record(record: Dict) -> Dict:
    now = datetime.now(timezone.utc)
    record["ingestion_timestamp"] = now.isoformat()
    record["processing_id"]       = f"PROC-{uuid.uuid4().hex[:8].upper()}"
    record["environment"]         = ENVIRONMENT
    record["fraud_signals"]       = detect_fraud_signals(record)
    record["is_fraud_flagged"]    = len(record["fraud_signals"]) > 0

    try:
        ts = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
        record["year"]  = ts.year
        record["month"] = ts.month
        record["day"]   = ts.day
        record["hour"]  = ts.hour
    except Exception:
        record["year"]  = now.year
        record["month"] = now.month
        record["day"]   = now.day
        record["hour"]  = now.hour

    return record


def write_bronze(records: List[Dict], batch_id: str) -> str:
    if not records:
        return ""
    now = datetime.now(timezone.utc)
    key = (f"bronze/payments/year={now.year}/month={now.month:02d}/"
           f"day={now.day:02d}/hour={now.hour:02d}/"
           f"payments_{batch_id}_{now.strftime('%H%M%S')}.json")
    payload = "\n".join(json.dumps(r, default=str) for r in records)
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={"record_count": str(len(records)), "batch_id": batch_id},
    )
    return f"s3://{S3_BUCKET}/{key}"


def send_dlq(record: Dict, reason: str) -> None:
    now = datetime.now(timezone.utc)
    key = (f"dlq/payments/year={now.year}/month={now.month:02d}/"
           f"day={now.day:02d}/failed_{uuid.uuid4().hex[:8]}.json")
    dlq_rec = {"original_record": record, "failure_reason": reason,
                "failed_at": now.isoformat(), "stream": "payments"}
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key,
                             Body=json.dumps(dlq_rec, default=str).encode(),
                             ContentType="application/json")
    except ClientError as e:
        logger.error(f"DLQ write failed: {e}")


def send_alert(subject: str, message: str) -> None:
    if not SNS_ALERT_ARN:
        return
    try:
        sns_client.publish(TopicArn=SNS_ALERT_ARN,
                           Subject=f"[{ENVIRONMENT.upper()}] {subject}",
                           Message=message)
    except ClientError:
        pass


# ─────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────

def lambda_handler(event: Dict, context: Any) -> Dict:
    records    = event.get("Records", [])
    batch_id   = context.aws_request_id if context else uuid.uuid4().hex
    valid, failed, duplicates, fraud_flagged = [], [], 0, 0

    logger.info(f"Processing payments batch | id={batch_id} | count={len(records)}")

    for rec in records:
        raw = None
        try:
            raw = json.loads(base64.b64decode(rec["kinesis"]["data"]).decode("utf-8"))

            ok, err = validate_schema(raw)
            if not ok:
                failed.append((raw, f"Schema: {err}"))
                continue

            ok, err = validate_payment(raw)
            if not ok:
                failed.append((raw, f"Validation: {err}"))
                continue

            if check_duplicate(raw["payment_id"]):
                duplicates += 1
                continue

            enriched = enrich_record(raw)
            if enriched.get("is_fraud_flagged"):
                fraud_flagged += 1
                logger.warning(f"Fraud signals on {raw['payment_id']}: {enriched['fraud_signals']}")

            valid.append(enriched)

        except json.JSONDecodeError as e:
            failed.append(({}, f"JSON decode: {e}"))
        except Exception as e:
            logger.exception(f"Processing error: {e}")
            failed.append((raw or {}, f"Error: {e}"))

    s3_path = ""
    if valid:
        try:
            s3_path = write_bronze(valid, batch_id)
        except Exception as e:
            logger.error(f"S3 write failed: {e}")
            send_alert("Payments Lambda: S3 Write Failure", str(e))

    for rec, reason in failed:
        send_dlq(rec, reason)

    total        = len(records)
    failure_rate = len(failed) / total if total > 0 else 0
    if failure_rate > 0.10 and total >= 10:
        send_alert("Payments Lambda: High Failure Rate",
                   f"Batch {batch_id}: {failure_rate:.1%} failure ({len(failed)}/{total})")

    result = {
        "statusCode": 200, "batch_id": batch_id,
        "total_records": total,
        "valid_records": len(valid),
        "failed_records": len(failed),
        "duplicate_records": duplicates,
        "fraud_flagged": fraud_flagged,
        "failure_rate_pct": round(failure_rate * 100, 2),
        "s3_path": s3_path,
    }
    logger.info(f"Batch complete | {json.dumps(result)}")
    return result
