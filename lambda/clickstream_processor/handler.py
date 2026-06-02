"""
AWS Lambda: Clickstream Stream Processor
==========================================
Processes clickstream/page-view events from Kinesis.
High-throughput handler optimised for the largest event volume stream.

Author: Data Engineering Team
Runtime: Python 3.11
Memory: 256 MB   (clickstream is lightweight)
Timeout: 180s
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

S3_BUCKET     = os.environ["S3_BUCKET"]
SNS_ALERT_ARN = os.environ.get("SNS_ALERT_ARN", "")
ENVIRONMENT   = os.environ.get("ENVIRONMENT", "prod")

REQUIRED_FIELDS = {"event_id", "session_id", "user_id", "page_type", "event_timestamp"}
VALID_PAGE_TYPES = {
    "home", "category", "product_detail", "search_results",
    "cart", "checkout", "order_confirmation", "account", "wishlist",
}


def validate(record: Dict) -> Tuple[bool, Optional[str]]:
    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        return False, f"Missing: {missing}"
    if record.get("page_type") not in VALID_PAGE_TYPES:
        # Warn but don't reject — new page types can be added at any time
        logger.debug(f"Unknown page_type: {record.get('page_type')}")
    try:
        datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, "Invalid timestamp"
    return True, None


def enrich(record: Dict) -> Dict:
    now = datetime.now(timezone.utc)
    record["ingestion_timestamp"] = now.isoformat()
    record["environment"]         = ENVIRONMENT
    try:
        ts = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
        record["year"]  = ts.year
        record["month"] = ts.month
        record["day"]   = ts.day
        record["hour"]  = ts.hour
    except Exception:
        record.update({"year": now.year, "month": now.month, "day": now.day, "hour": now.hour})
    return record


def write_bronze(records: List[Dict], batch_id: str) -> str:
    if not records:
        return ""
    now = datetime.now(timezone.utc)
    key = (f"bronze/clickstream/year={now.year}/month={now.month:02d}/"
           f"day={now.day:02d}/hour={now.hour:02d}/"
           f"clicks_{batch_id}_{now.strftime('%H%M%S%f')}.json")
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=("\n".join(json.dumps(r, default=str) for r in records)).encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={"record_count": str(len(records))},
    )
    return f"s3://{S3_BUCKET}/{key}"


def send_dlq(record: Dict, reason: str) -> None:
    now = datetime.now(timezone.utc)
    key = f"dlq/clickstream/year={now.year}/month={now.month:02d}/day={now.day:02d}/failed_{uuid.uuid4().hex[:8]}.json"
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=json.dumps({"original_record": record, "reason": reason, "failed_at": now.isoformat()}, default=str).encode(),
        )
    except ClientError:
        pass


def lambda_handler(event: Dict, context: Any) -> Dict:
    records  = event.get("Records", [])
    batch_id = context.aws_request_id if context else uuid.uuid4().hex
    valid, failed = [], []

    for rec in records:
        raw = None
        try:
            raw = json.loads(base64.b64decode(rec["kinesis"]["data"]).decode("utf-8"))
            ok, err = validate(raw)
            if not ok:
                failed.append((raw, err))
                continue
            valid.append(enrich(raw))
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
        "failed": len(failed), "s3_path": s3_path,
    }
    logger.info(f"Clickstream batch done | {json.dumps(result)}")
    return result
