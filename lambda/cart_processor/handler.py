"""
AWS Lambda: Cart Stream Processor
===================================
Processes cart activity events (add, remove, abandon, save).
Tracks cart lifecycle and routes abandonment events for remarketing.

Author: Data Engineering Team
Runtime: Python 3.11
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

S3_BUCKET             = os.environ["S3_BUCKET"]
SNS_ALERT_ARN         = os.environ.get("SNS_ALERT_ARN", "")
ABANDONMENT_TOPIC_ARN = os.environ.get("ABANDONMENT_TOPIC_ARN", "")
ENVIRONMENT           = os.environ.get("ENVIRONMENT", "prod")

REQUIRED_FIELDS = {"event_id", "session_id", "user_id", "cart_id", "action",
                   "product_id", "unit_price", "quantity", "event_timestamp"}
VALID_ACTIONS   = {
    "item_added", "item_removed", "quantity_updated",
    "cart_viewed", "cart_abandoned", "cart_saved",
}


def validate(record: Dict) -> Tuple[bool, Optional[str]]:
    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        return False, f"Missing: {missing}"
    if record.get("action") not in VALID_ACTIONS:
        return False, f"Invalid action: {record.get('action')}"
    if not isinstance(record.get("unit_price"), (int, float)) or record.get("unit_price", 0) <= 0:
        return False, f"Invalid unit_price: {record.get('unit_price')}"
    return True, None


def enrich(record: Dict) -> Dict:
    now = datetime.now(timezone.utc)
    record["ingestion_timestamp"] = now.isoformat()
    record["environment"]         = ENVIRONMENT
    record["cart_value"]          = round(
        record.get("unit_price", 0) * record.get("quantity", 1), 2
    )
    try:
        ts = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
        record.update({"year": ts.year, "month": ts.month, "day": ts.day, "hour": ts.hour})
    except Exception:
        record.update({"year": now.year, "month": now.month, "day": now.day, "hour": now.hour})
    return record


def notify_abandonment(record: Dict) -> None:
    """Publish cart abandonment to SNS for remarketing downstream."""
    if not ABANDONMENT_TOPIC_ARN:
        return
    try:
        sns_client.publish(
            TopicArn=ABANDONMENT_TOPIC_ARN,
            Subject="Cart Abandoned",
            Message=json.dumps({
                "user_id":     record.get("user_id"),
                "session_id":  record.get("session_id"),
                "cart_id":     record.get("cart_id"),
                "cart_total":  record.get("cart_total"),
                "abandoned_at": record.get("event_timestamp"),
                "country":     record.get("country"),
            }, default=str),
            MessageAttributes={
                "event_type": {"DataType": "String", "StringValue": "cart_abandoned"},
            },
        )
    except ClientError as e:
        logger.warning(f"Abandonment notification failed: {e}")


def write_bronze(records: List[Dict], batch_id: str) -> str:
    if not records:
        return ""
    now = datetime.now(timezone.utc)
    key = (f"bronze/carts/year={now.year}/month={now.month:02d}/"
           f"day={now.day:02d}/hour={now.hour:02d}/"
           f"carts_{batch_id}_{now.strftime('%H%M%S')}.json")
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=("\n".join(json.dumps(r, default=str) for r in records)).encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={"record_count": str(len(records))},
    )
    return f"s3://{S3_BUCKET}/{key}"


def send_dlq(record: Dict, reason: str) -> None:
    now = datetime.now(timezone.utc)
    key = f"dlq/carts/year={now.year}/month={now.month:02d}/day={now.day:02d}/failed_{uuid.uuid4().hex[:8]}.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps({"original_record": record, "reason": reason,
                             "failed_at": now.isoformat()}, default=str).encode(),
        )
    except ClientError:
        pass


def lambda_handler(event: Dict, context: Any) -> Dict:
    records  = event.get("Records", [])
    batch_id = context.aws_request_id if context else uuid.uuid4().hex
    valid, failed, abandonments = [], [], 0

    for rec in records:
        raw = None
        try:
            raw = json.loads(base64.b64decode(rec["kinesis"]["data"]).decode("utf-8"))
            ok, err = validate(raw)
            if not ok:
                failed.append((raw, err))
                continue
            enriched = enrich(raw)
            if enriched.get("action") == "cart_abandoned":
                notify_abandonment(enriched)
                abandonments += 1
            valid.append(enriched)
        except json.JSONDecodeError as e:
            failed.append(({}, f"JSON: {e}"))
        except Exception as e:
            failed.append((raw or {}, str(e)))

    s3_path = ""
    if valid:
        try:
            s3_path = write_bronze(valid, batch_id)
        except Exception as e:
            logger.error(f"S3 write failed: {e}")

    for rec, reason in failed:
        send_dlq(rec, reason)

    result = {
        "statusCode": 200, "batch_id": batch_id,
        "total": len(records), "valid": len(valid),
        "failed": len(failed), "abandonments_notified": abandonments,
        "s3_path": s3_path,
    }
    logger.info(f"Cart batch done | {json.dumps(result)}")
    return result
