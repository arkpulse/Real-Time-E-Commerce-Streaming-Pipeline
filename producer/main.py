"""
Main Streaming Orchestrator
=============================
Orchestrates the continuous generation and streaming of e-commerce events
to AWS Kinesis streams. Supports configurable event rates and graceful shutdown.

Author: Data Engineering Team
Version: 1.0.0
"""

import json
import logging
import os
import random
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from event_generator import EventGenerator, get_generator
from kinesis_producer import KinesisConfig, KinesisProducer

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("StreamOrchestrator")


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "50"))
METRICS_INTERVAL_SECONDS = int(os.getenv("METRICS_INTERVAL", "30"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

# Event distribution weights (must sum to 1.0)
EVENT_WEIGHTS = {
    "clickstream": 0.50,   # Most frequent — page views
    "cart": 0.25,           # Cart interactions
    "order": 0.15,          # Order placements
    "payment": 0.10,        # Payment events
}

# ─────────────────────────────────────────────
# Graceful Shutdown
# ─────────────────────────────────────────────

_running = True


def signal_handler(signum, frame):
    global _running
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    _running = False


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


# ─────────────────────────────────────────────
# Streaming Logic
# ─────────────────────────────────────────────

class StreamOrchestrator:
    """
    Manages continuous event generation and Kinesis stream routing.
    Uses weighted random selection to simulate realistic event distribution.
    """

    def __init__(self):
        self.config = KinesisConfig()
        self.producer = KinesisProducer(self.config)
        self.generator = get_generator()
        self._event_count = 0
        self._last_metrics_report = time.time()
        logger.info("StreamOrchestrator initialized")
        logger.info(f"Config: events_per_second={EVENTS_PER_SECOND}, workers={MAX_WORKERS}")

    def _select_event_type(self) -> str:
        """Weighted random selection of event type."""
        event_types = list(EVENT_WEIGHTS.keys())
        weights = list(EVENT_WEIGHTS.values())
        return random.choices(event_types, weights=weights, k=1)[0]

    def _generate_and_send(self) -> bool:
        """Generate one event and send to appropriate Kinesis stream."""
        try:
            event_type = self._select_event_type()

            if event_type == "order":
                event = self.generator.generate_order_event()
                stream = self.config.orders_stream
            elif event_type == "payment":
                event = self.generator.generate_payment_event()
                stream = self.config.payments_stream
            elif event_type == "clickstream":
                event = self.generator.generate_clickstream_event()
                stream = self.config.clickstream_stream
            elif event_type == "cart":
                event = self.generator.generate_cart_event()
                stream = self.config.cart_stream
            else:
                logger.warning(f"Unknown event type: {event_type}")
                return False

            self.producer.send_event(stream, event)
            self._event_count += 1

            if self._event_count % 100 == 0:
                logger.debug(f"Events generated: {self._event_count}")

            return True

        except Exception as e:
            logger.exception(f"Error generating/sending event: {e}")
            return False

    def _report_metrics(self) -> None:
        """Log current producer metrics at configured interval."""
        now = time.time()
        if (now - self._last_metrics_report) >= METRICS_INTERVAL_SECONDS:
            metrics = self.producer.get_metrics()
            logger.info(f"Producer Metrics | {json.dumps(metrics)}")
            self._last_metrics_report = now

    def run_single_threaded(self) -> None:
        """Run event generation in a single thread (for lower throughput)."""
        logger.info("Starting single-threaded streaming mode")
        sleep_interval = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0

        while _running:
            self._generate_and_send()
            self._report_metrics()
            if sleep_interval > 0:
                time.sleep(sleep_interval)

    def run_multi_threaded(self) -> None:
        """
        Run event generation using ThreadPoolExecutor for higher throughput.
        Each worker generates events continuously.
        """
        logger.info(f"Starting multi-threaded streaming mode | workers={MAX_WORKERS}")
        sleep_per_worker = MAX_WORKERS / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0

        def worker_loop():
            while _running:
                self._generate_and_send()
                if sleep_per_worker > 0:
                    time.sleep(sleep_per_worker)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(worker_loop) for _ in range(MAX_WORKERS)]
            while _running:
                self._report_metrics()
                time.sleep(1)

            # Wait for all workers to finish
            for future in as_completed(futures, timeout=10):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker error: {e}")

    def shutdown(self) -> None:
        """Graceful shutdown: flush buffers and log final stats."""
        logger.info(f"Shutting down after {self._event_count} total events")
        self.producer.close()
        logger.info("StreamOrchestrator shutdown complete")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  Real-Time E-Commerce Streaming Producer")
    logger.info("  Version: 1.0.0")
    logger.info("=" * 60)

    mode = os.getenv("STREAMING_MODE", "multi")
    orchestrator = StreamOrchestrator()

    try:
        if mode == "single":
            orchestrator.run_single_threaded()
        else:
            orchestrator.run_multi_threaded()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        orchestrator.shutdown()
        logger.info("Producer process exited cleanly")
        sys.exit(0)


if __name__ == "__main__":
    main()
