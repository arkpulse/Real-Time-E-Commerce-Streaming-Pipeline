"""
AWS Lambda: Dead Letter Queue Handler
=======================================
Processes failed records from the DLQ S3 path.
Parses failure reasons, sends structured alerts,
and optionally re-drives recoverable records.

Author: Data Engineering Team
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")

S3_BUCKET     = os.environ["S3_BUCKET"]
SNS_ALERT_ARN = os.environ.get("SNS_ALERT_ARN", "")
ENVIRONMENT   = os.environ.get("ENVIRONMENT", "prod")

# Recoverable error classes (can be re-driven)
RECOVERABLE_ERRORS = {
    "ProvisionedThroughputExceededException",
    "ServiceUnavailableException",
    "S3 write failed",
}


def is_recoverable(reason: str) -> bool:
    return any(err in reason for err in RECOVERABLE_ERRORS)


def send_alert(subject: str, body: str) -> None:
    if not SNS_ALERT_ARN:
        return
    try:
        sns_client.publish(
            TopicArn=SNS_ALERT_ARN,
            Subject=f"[{ENVIRONMENT.upper()}] DLQ: {subject}",
            Message=body,
        )
    except ClientError as e:
        logger.error(f"SNS publish failed: {e}")


def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Triggered by S3 event on the dlq/ prefix.
    Reads each DLQ record, classifies, and alerts.
    """
    processed = failed_permanent = recoverable = 0

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]

            obj     = s3_client.get_object(Bucket=bucket, Key=key)
            content = json.loads(obj["Body"].read().decode("utf-8"))

            reason  = content.get("failure_reason", "unknown")
            stream  = content.get("stream", "unknown")
            orig    = content.get("original_record", {})

            logger.warning(
                f"DLQ record | stream={stream} | reason={reason} | "
                f"record_id={orig.get('event_id', orig.get('order_id', 'unknown'))}"
            )

            if is_recoverable(reason):
                recoverable += 1
                logger.info(f"Recoverable error — flagged for re-drive: {key}")
            else:
                failed_permanent += 1
                send_alert(
                    f"Permanent failure in {stream}",
                    f"Key: {key}\nReason: {reason}\n"
                    f"Record: {json.dumps(orig, default=str)[:500]}",
                )

            processed += 1

        except Exception as e:
            logger.exception(f"Error processing DLQ record: {e}")

    result = {
        "statusCode": 200,
        "processed": processed,
        "permanent_failures": failed_permanent,
        "recoverable": recoverable,
    }
    logger.info(f"DLQ handler complete | {json.dumps(result)}")
    return result
