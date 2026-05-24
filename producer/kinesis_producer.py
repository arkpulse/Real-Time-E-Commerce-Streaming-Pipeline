"""
AWS Kinesis Streaming Producer
================================
High-throughput producer that sends e-commerce events to multiple
Kinesis Data Streams with batching, retry logic, and comprehensive error handling.

Author: Data Engineering Team
Version: 1.0.0
"""

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("KinesisProducer")


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

@dataclass
class KinesisConfig:
    region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    max_retries: int = field(default_factory=lambda: int(os.getenv("KINESIS_MAX_RETRIES", "3")))
    retry_delay_seconds: float = field(default_factory=lambda: float(os.getenv("KINESIS_RETRY_DELAY", "1.0")))
    batch_size: int = field(default_factory=lambda: int(os.getenv("KINESIS_BATCH_SIZE", "100")))  # Max 500
    flush_interval_seconds: float = field(default_factory=lambda: float(os.getenv("KINESIS_FLUSH_INTERVAL", "5.0")))

    # Stream names
    orders_stream: str = field(default_factory=lambda: os.getenv("ORDERS_STREAM", "ecommerce-orders-stream"))
    payments_stream: str = field(default_factory=lambda: os.getenv("PAYMENTS_STREAM", "ecommerce-payments-stream"))
    clickstream_stream: str = field(default_factory=lambda: os.getenv("CLICKSTREAM_STREAM", "ecommerce-clickstream-stream"))
    cart_stream: str = field(default_factory=lambda: os.getenv("CART_STREAM", "ecommerce-cart-stream"))


# ─────────────────────────────────────────────
# Metrics Tracking
# ─────────────────────────────────────────────

class ProducerMetrics:
    """In-memory metrics tracker for producer operations."""

    def __init__(self):
        self.records_sent = defaultdict(int)
        self.records_failed = defaultdict(int)
        self.bytes_sent = defaultdict(int)
        self.api_calls = defaultdict(int)
        self.throttle_count = defaultdict(int)
        self.start_time = time.time()

    def report(self) -> Dict[str, Any]:
        elapsed = time.time() - self.start_time
        total_sent = sum(self.records_sent.values())
        return {
            "elapsed_seconds": round(elapsed, 2),
            "total_records_sent": total_sent,
            "throughput_rps": round(total_sent / elapsed, 2) if elapsed > 0 else 0,
            "records_per_stream": dict(self.records_sent),
            "failures_per_stream": dict(self.records_failed),
            "bytes_per_stream": dict(self.bytes_sent),
            "api_calls_per_stream": dict(self.api_calls),
            "throttles_per_stream": dict(self.throttle_count),
        }


# ─────────────────────────────────────────────
# Kinesis Producer
# ─────────────────────────────────────────────

class KinesisProducer:
    """
    Production-grade Kinesis Data Streams producer.
    Implements batching, exponential backoff retries, and per-stream buffering.
    """

    def __init__(self, config: Optional[KinesisConfig] = None):
        self.config = config or KinesisConfig()
        self.metrics = ProducerMetrics()
        self._buffers: Dict[str, List[Dict]] = defaultdict(list)
        self._last_flush: Dict[str, float] = defaultdict(float)

        boto_config = Config(
            region_name=self.config.region,
            retries={"max_attempts": 2, "mode": "adaptive"},
            max_pool_connections=50,
        )
        self.client = boto3.client("kinesis", config=boto_config)
        logger.info(f"KinesisProducer initialized | region={self.config.region}")

    def _serialize_record(self, event: Dict[str, Any]) -> Tuple[bytes, str]:
        """Serialize event to JSON bytes and derive partition key."""
        payload = json.dumps(event, default=str).encode("utf-8")
        # Use user_id as partition key for even shard distribution
        partition_key = event.get("user_id", event.get("event_id", "default"))
        return payload, str(partition_key)

    def _send_batch(self, stream_name: str, records: List[Dict]) -> List[Dict]:
        """
        Send a batch of records to Kinesis using put_records API.
        Returns list of failed records for retry.
        """
        kinesis_records = []
        for record in records:
            payload, partition_key = self._serialize_record(record)
            kinesis_records.append({
                "Data": payload,
                "PartitionKey": partition_key,
            })

        self.metrics.api_calls[stream_name] += 1
        failed_records = []

        try:
            response = self.client.put_records(
                StreamName=stream_name,
                Records=kinesis_records,
            )

            failed_count = response.get("FailedRecordCount", 0)
            if failed_count > 0:
                logger.warning(f"Partial failure | stream={stream_name} | failed={failed_count}/{len(records)}")
                for idx, result in enumerate(response["Records"]):
                    if "ErrorCode" in result:
                        error_code = result["ErrorCode"]
                        if error_code == "ProvisionedThroughputExceededException":
                            self.metrics.throttle_count[stream_name] += 1
                        failed_records.append(records[idx])
                        logger.debug(f"Record failed | stream={stream_name} | error={error_code} | msg={result.get('ErrorMessage')}")
            else:
                # All records succeeded
                success_count = len(records) - failed_count
                self.metrics.records_sent[stream_name] += success_count
                self.metrics.bytes_sent[stream_name] += sum(
                    len(json.dumps(r, default=str).encode()) for r in records
                )
                logger.debug(f"Batch sent | stream={stream_name} | count={success_count}")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"Kinesis ClientError | stream={stream_name} | error={error_code} | msg={str(e)}")
            self.metrics.records_failed[stream_name] += len(records)
            failed_records = records  # Retry all

        return failed_records

    def _send_with_retry(self, stream_name: str, records: List[Dict]) -> bool:
        """Send records with exponential backoff retry logic."""
        pending = records
        for attempt in range(self.config.max_retries):
            if not pending:
                return True

            failed = self._send_batch(stream_name, pending)
            if not failed:
                return True

            # Exponential backoff
            delay = self.config.retry_delay_seconds * (2 ** attempt) + (time.time() % 0.1)
            logger.info(f"Retry {attempt + 1}/{self.config.max_retries} | stream={stream_name} | delay={delay:.2f}s | records={len(failed)}")
            time.sleep(delay)
            pending = failed

        # Final failure
        self.metrics.records_failed[stream_name] += len(pending)
        logger.error(f"Records permanently failed | stream={stream_name} | count={len(pending)}")
        return False

    def buffer_event(self, stream_name: str, event: Dict[str, Any]) -> None:
        """Add event to stream buffer. Flushes automatically when batch is full."""
        self._buffers[stream_name].append(event)

        if len(self._buffers[stream_name]) >= self.config.batch_size:
            self.flush_stream(stream_name)

    def flush_stream(self, stream_name: str) -> None:
        """Flush all buffered records for a stream."""
        if not self._buffers[stream_name]:
            return

        batch = self._buffers[stream_name][:]
        self._buffers[stream_name] = []
        self._last_flush[stream_name] = time.time()

        # Chunk into max 500 records per API call
        chunk_size = 500
        chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
        for chunk in chunks:
            self._send_with_retry(stream_name, chunk)

    def flush_all(self) -> None:
        """Flush all stream buffers."""
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)

    def should_flush(self, stream_name: str) -> bool:
        """Check if a stream buffer should be flushed based on time interval."""
        last = self._last_flush.get(stream_name, 0)
        return (time.time() - last) >= self.config.flush_interval_seconds

    def send_event(self, stream_name: str, event: Dict[str, Any]) -> None:
        """
        Primary method to send a single event.
        Buffers and flushes based on batch/time thresholds.
        """
        self.buffer_event(stream_name, event)
        if self.should_flush(stream_name):
            self.flush_stream(stream_name)

    def send_immediate(self, stream_name: str, event: Dict[str, Any]) -> bool:
        """Send a single event immediately without buffering (for critical events)."""
        payload, partition_key = self._serialize_record(event)
        for attempt in range(self.config.max_retries):
            try:
                self.client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=partition_key,
                )
                self.metrics.records_sent[stream_name] += 1
                return True
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if attempt < self.config.max_retries - 1:
                    delay = self.config.retry_delay_seconds * (2 ** attempt)
                    logger.warning(f"Retry immediate send | attempt={attempt+1} | error={error_code} | delay={delay}s")
                    time.sleep(delay)
                else:
                    logger.error(f"Immediate send failed | stream={stream_name} | error={error_code}")
                    self.metrics.records_failed[stream_name] += 1
                    return False
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Return current producer metrics."""
        return self.metrics.report()

    def close(self) -> None:
        """Gracefully shutdown: flush all buffers and log final metrics."""
        logger.info("Shutting down KinesisProducer — flushing all buffers...")
        self.flush_all()
        metrics = self.get_metrics()
        logger.info(f"Final producer metrics: {json.dumps(metrics, indent=2)}")
