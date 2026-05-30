"""
Integration Tests: End-to-End Pipeline (LocalStack)
=====================================================
Tests the full streaming pipeline flow:
Producer → Kinesis → Lambda → S3 → DQ checks

Requires LocalStack running:
    docker compose --profile local-dev up -d localstack

Run with:
    pytest tests/integration/ -v -m integration \
        --timeout=120 \
        -s

Author: Data Engineering Team
"""

import base64
import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
import pytest

# ── LocalStack endpoint ────────────────────────────────────

ENDPOINT   = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET     = os.getenv("S3_BUCKET", "ecommerce-data-lake")
ENV        = "local"

AWS_KWARGS = dict(
    endpoint_url=ENDPOINT,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

pytestmark = pytest.mark.integration


# ── Fixtures ───────────────────────────────────────────────

@pytest.fixture(scope="session")
def kinesis():
    return boto3.client("kinesis", **AWS_KWARGS)

@pytest.fixture(scope="session")
def s3():
    return boto3.client("s3", **AWS_KWARGS)

@pytest.fixture(scope="session")
def dynamodb():
    return boto3.resource("dynamodb", **AWS_KWARGS)

@pytest.fixture(scope="session")
def sns():
    return boto3.client("sns", **AWS_KWARGS)


def wait_for_s3_key(s3_client, bucket: str, prefix: str,
                    timeout: int = 60, poll: int = 3) -> str | None:
    """Poll S3 for a key matching prefix. Returns key or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objs = resp.get("Contents", [])
        if objs:
            return objs[0]["Key"]
        time.sleep(poll)
    return None


# ── LocalStack health check ────────────────────────────────

def test_localstack_healthy(kinesis):
    """Verify LocalStack is reachable before running any tests."""
    streams = kinesis.list_streams()
    assert "StreamNames" in streams, "LocalStack Kinesis not responding"


# ── Kinesis Stream Tests ────────────────────────────────────

class TestKinesisStreams:

    def test_orders_stream_exists(self, kinesis):
        streams = kinesis.list_streams()["StreamNames"]
        assert "ecommerce-orders-stream" in streams

    def test_payments_stream_exists(self, kinesis):
        streams = kinesis.list_streams()["StreamNames"]
        assert "ecommerce-payments-stream" in streams

    def test_clickstream_stream_exists(self, kinesis):
        streams = kinesis.list_streams()["StreamNames"]
        assert "ecommerce-clickstream-stream" in streams

    def test_cart_stream_exists(self, kinesis):
        streams = kinesis.list_streams()["StreamNames"]
        assert "ecommerce-cart-stream" in streams

    def test_put_record_orders(self, kinesis):
        payload = json.dumps({
            "event_id":        f"EVT-{uuid.uuid4().hex[:8].upper()}",
            "event_type":      "order_placed",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "order_id":        f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "user_id":         "USR-INT001",
            "session_id":      "SES-INT001",
            "total_amount":    99.99,
            "currency":        "USD",
            "country":         "US",
            "payment_method":  "credit_card",
            "order_status":    "confirmed",
            "items":           [{"product_id": "PRD-00001", "quantity": 1,
                                 "unit_price": 99.99, "final_price": 99.99,
                                 "product_name": "Test", "category": "Electronics",
                                 "brand": "Brand", "discount_pct": 0}],
        }).encode("utf-8")

        resp = kinesis.put_record(
            StreamName="ecommerce-orders-stream",
            Data=payload,
            PartitionKey="USR-INT001",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "SequenceNumber" in resp

    def test_put_batch_records(self, kinesis):
        records = []
        for i in range(10):
            payload = json.dumps({
                "event_id":        f"EVT-BATCH-{i:04d}",
                "event_type":      "page_view",
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id":      f"SES-BATCH-{i:04d}",
                "user_id":         f"USR-BATCH-{i:04d}",
                "page_type":       "home",
            }).encode("utf-8")
            records.append({"Data": payload, "PartitionKey": f"USR-BATCH-{i}"})

        resp = kinesis.put_records(
            StreamName="ecommerce-clickstream-stream",
            Records=records,
        )
        assert resp["FailedRecordCount"] == 0
        assert len(resp["Records"]) == 10


# ── S3 Data Lake Tests ─────────────────────────────────────

class TestS3DataLake:

    def test_bucket_exists(self, s3):
        resp = s3.list_buckets()
        buckets = [b["Name"] for b in resp["Buckets"]]
        assert BUCKET in buckets

    def test_bronze_prefix_accessible(self, s3):
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/", MaxKeys=1)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_write_and_read_bronze_object(self, s3):
        now    = datetime.now(timezone.utc)
        key    = (f"bronze/orders/year={now.year}/month={now.month:02d}/"
                  f"day={now.day:02d}/hour={now.hour:02d}/test_integration.json")
        record = {"order_id": "ORD-INTTEST", "total_amount": 42.00,
                  "test": True, "timestamp": now.isoformat()}

        # Write
        s3.put_object(
            Bucket=BUCKET, Key=key,
            Body=json.dumps(record).encode("utf-8"),
            ContentType="application/json",
        )

        # Read back
        obj     = s3.get_object(Bucket=BUCKET, Key=key)
        content = json.loads(obj["Body"].read().decode("utf-8"))
        assert content["order_id"] == "ORD-INTTEST"
        assert content["total_amount"] == 42.00

        # Cleanup
        s3.delete_object(Bucket=BUCKET, Key=key)

    def test_dlq_prefix_accessible(self, s3):
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="dlq/")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


# ── DynamoDB Dedup Tests ───────────────────────────────────

class TestDynamoDBDedup:

    def test_orders_dedup_table_exists(self, dynamodb):
        table = dynamodb.Table("ecommerce-dedup-orders")
        assert table.table_status in ("ACTIVE", "CREATING")

    def test_payments_dedup_table_exists(self, dynamodb):
        table = dynamodb.Table("ecommerce-dedup-payments")
        assert table.table_status in ("ACTIVE", "CREATING")

    def test_put_and_get_dedup_record(self, dynamodb):
        table    = dynamodb.Table("ecommerce-dedup-orders")
        order_id = f"ORD-DEDUP-{uuid.uuid4().hex[:8].upper()}"
        ttl      = int(datetime.now(timezone.utc).timestamp()) + 86400

        # Write
        table.put_item(Item={
            "order_id":     order_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "ttl":          ttl,
        })

        # Read back — should exist (duplicate)
        resp = table.get_item(Key={"order_id": order_id}, ConsistentRead=True)
        assert "Item" in resp
        assert resp["Item"]["order_id"] == order_id

        # New ID — should not exist
        new_id = f"ORD-NEW-{uuid.uuid4().hex[:8].upper()}"
        resp2  = table.get_item(Key={"order_id": new_id}, ConsistentRead=True)
        assert "Item" not in resp2


# ── SNS Alert Tests ────────────────────────────────────────

class TestSNSAlerts:

    def test_alert_topic_exists(self, sns):
        resp   = sns.list_topics()
        topics = [t["TopicArn"] for t in resp["Topics"]]
        assert any("pipeline-alerts" in t for t in topics)

    def test_publish_alert(self, sns):
        resp   = sns.list_topics()
        topics = [t["TopicArn"] for t in resp["Topics"]]
        alert_topic = next(t for t in topics if "pipeline-alerts" in t)

        pub_resp = sns.publish(
            TopicArn=alert_topic,
            Subject="[TEST] Integration test alert",
            Message="This is a test alert from the integration test suite.",
        )
        assert pub_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "MessageId" in pub_resp


# ── End-to-End Producer → Kinesis Flow ─────────────────────

class TestProducerToKinesis:

    def test_event_generator_produces_valid_events(self):
        """EventGenerator produces valid serialisable events."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../producer"))
        from event_generator import EventGenerator

        gen = EventGenerator()
        for event_fn in [
            gen.generate_order_event,
            gen.generate_payment_event,
            gen.generate_clickstream_event,
            gen.generate_cart_event,
        ]:
            event   = event_fn()
            payload = json.dumps(event, default=str)
            parsed  = json.loads(payload)
            assert "event_id" in parsed
            assert "event_timestamp" in parsed

    def test_kinesis_producer_send_immediate(self, kinesis):
        """KinesisProducer.send_immediate delivers a single event."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../producer"))
        from kinesis_producer import KinesisConfig, KinesisProducer

        cfg            = KinesisConfig()
        cfg.orders_stream = "ecommerce-orders-stream"

        # Monkey-patch the boto3 client to use LocalStack
        producer       = KinesisProducer(cfg)
        producer.client = boto3.client("kinesis", **AWS_KWARGS)

        event = {
            "event_id":        f"EVT-{uuid.uuid4().hex[:8].upper()}",
            "event_type":      "order_placed",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "order_id":        f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "user_id":         "USR-PROD001",
        }

        result = producer.send_immediate("ecommerce-orders-stream", event)
        assert result is True
        assert producer.metrics.records_sent.get("ecommerce-orders-stream", 0) == 1

    def test_kinesis_producer_buffer_and_flush(self, kinesis):
        """KinesisProducer buffers events and flushes correctly."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../producer"))
        from kinesis_producer import KinesisConfig, KinesisProducer

        cfg            = KinesisConfig()
        cfg.batch_size = 5
        producer       = KinesisProducer(cfg)
        producer.client = boto3.client("kinesis", **AWS_KWARGS)

        stream = "ecommerce-clickstream-stream"
        for i in range(5):
            producer.buffer_event(stream, {
                "event_id":        f"EVT-BUF-{i:04d}",
                "event_type":      "page_view",
                "event_timestamp": datetime.now(timezone.utc).isoformat(),
                "session_id":      f"SES-{i:04d}",
                "user_id":         f"USR-{i:04d}",
            })

        # After 5 events (== batch_size), buffer should have auto-flushed
        assert len(producer._buffers.get(stream, [])) == 0
        producer.close()
