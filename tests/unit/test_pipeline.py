"""
Unit Tests: Event Generator & Data Quality Framework
=====================================================
Comprehensive test suite using pytest and moto for AWS mocking.

Author: Data Engineering Team
Run with: pytest tests/unit/ -v --cov=producer --cov=data_quality
"""

import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest

# ── Path setup ──────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../producer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../data_quality"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda/orders_processor"))


# =============================================================
# Event Generator Tests
# =============================================================

class TestEventGenerator:

    @pytest.fixture(autouse=True)
    def setup(self):
        from event_generator import EventGenerator
        self.gen = EventGenerator()

    def test_order_event_has_required_fields(self):
        event = self.gen.generate_order_event()
        required = {"event_id", "event_type", "order_id", "user_id", "session_id",
                    "total_amount", "items", "payment_method", "currency", "country"}
        for field in required:
            assert field in event, f"Missing required field: {field}"

    def test_order_event_positive_amount(self):
        for _ in range(20):
            event = self.gen.generate_order_event()
            assert event["total_amount"] > 0
            assert event["total_amount"] < 50000

    def test_order_has_items(self):
        for _ in range(10):
            event = self.gen.generate_order_event()
            assert isinstance(event["items"], list)
            assert len(event["items"]) >= 1

    def test_order_id_format(self):
        event = self.gen.generate_order_event()
        assert event["order_id"].startswith("ORD-")

    def test_user_id_format(self):
        event = self.gen.generate_order_event()
        assert event["user_id"].startswith("USR-")

    def test_session_id_format(self):
        event = self.gen.generate_order_event()
        assert event["session_id"].startswith("SES-")

    def test_payment_event_fields(self):
        event = self.gen.generate_payment_event()
        required = {"payment_id", "order_id", "amount", "payment_status",
                    "payment_method", "payment_gateway"}
        for field in required:
            assert field in event

    def test_payment_id_format(self):
        event = self.gen.generate_payment_event()
        assert event["payment_id"].startswith("PAY-")

    def test_payment_success_rate_approximate(self):
        """Test that success rate is roughly 94% (within ±10%)."""
        results = [self.gen.generate_payment_event()["payment_status"] for _ in range(500)]
        success_rate = results.count("success") / len(results)
        assert 0.84 <= success_rate <= 1.0, f"Unexpected success rate: {success_rate:.2%}"

    def test_clickstream_event_fields(self):
        event = self.gen.generate_clickstream_event()
        required = {"event_id", "session_id", "user_id", "page_type",
                    "time_on_page_seconds", "scroll_depth_pct"}
        for field in required:
            assert field in event

    def test_cart_event_fields(self):
        event = self.gen.generate_cart_event()
        required = {"event_id", "cart_id", "action", "product_id",
                    "unit_price", "quantity", "cart_total"}
        for field in required:
            assert field in event

    def test_event_timestamp_is_valid_iso(self):
        event = self.gen.generate_order_event()
        ts = event["event_timestamp"]
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert dt <= datetime.now(timezone.utc)

    def test_session_reuse(self):
        """Session should be reused across multiple events from the same generator."""
        events = [self.gen.generate_clickstream_event() for _ in range(50)]
        session_ids = {e["session_id"] for e in events}
        # With 200 max sessions and 50 events, we expect some reuse
        assert len(session_ids) < 50, "Expected some session reuse but got all unique"

    def test_event_id_uniqueness(self):
        events = [self.gen.generate_order_event() for _ in range(100)]
        event_ids = [e["event_id"] for e in events]
        assert len(set(event_ids)) == 100, "Event IDs should be unique"

    def test_device_types_valid(self):
        valid_devices = {"mobile", "desktop", "tablet"}
        for _ in range(30):
            event = self.gen.generate_clickstream_event()
            assert event["device_type"] in valid_devices

    def test_country_codes_valid(self):
        valid_countries = {"US","UK","CA","AU","DE","FR","JP","IN","BR","MX","SG","NL","SE","NO","AE"}
        for _ in range(30):
            event = self.gen.generate_order_event()
            assert event["country"] in valid_countries


# =============================================================
# Data Quality Framework Tests
# =============================================================

class TestDataQualityFramework:

    @pytest.fixture(autouse=True)
    def setup(self):
        from dq_framework import DataQualityEngine, Severity, CheckStatus
        self.DQEngine = DataQualityEngine
        self.Severity = Severity
        self.CheckStatus = CheckStatus

    def _valid_order(self) -> Dict:
        return {
            "event_id": "EVT-ABC123",
            "event_type": "order_placed",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "order_id": "ORD-XYZ789",
            "user_id": "USR-TEST01",
            "session_id": "SES-ABCDEF",
            "total_amount": 149.99,
            "subtotal": 135.99,
            "tax_amount": 10.88,
            "shipping_cost": 4.99,
            "discount_amount": 0.0,
            "currency": "USD",
            "country": "US",
            "payment_method": "credit_card",
            "order_status": "confirmed",
            "items": [
                {"product_id": "PRD-00001", "product_name": "Laptop",
                 "quantity": 1, "unit_price": 149.99, "final_price": 135.99}
            ],
        }

    def test_valid_order_passes_all_checks(self):
        engine = self.DQEngine("orders")
        report = engine.validate(self._valid_order())
        assert report.overall_status == "PASS", f"Failures: {[r for r in report.results if not r.passed]}"
        assert report.quality_score == 1.0

    def test_null_order_id_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["order_id"] = None
        report = engine.validate(record)
        assert report.overall_status in ("FAIL", "CRITICAL")
        failed_names = [r.check_name for r in report.results if not r.passed]
        assert "not_null.order_id" in failed_names

    def test_negative_amount_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["total_amount"] = -50.0
        report = engine.validate(record)
        assert report.overall_status in ("FAIL", "CRITICAL")

    def test_extreme_amount_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["total_amount"] = 99999.99
        report = engine.validate(record)
        assert any(not r.passed for r in report.results)

    def test_invalid_currency_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["currency"] = "XYZ"
        report = engine.validate(record)
        failed = [r.check_name for r in report.results if not r.passed]
        assert "valid_enum.currency" in failed

    def test_empty_items_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["items"] = []
        report = engine.validate(record)
        failed = [r.check_name for r in report.results if not r.passed]
        assert "order_items_not_empty" in failed

    def test_invalid_timestamp_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["event_timestamp"] = "not-a-timestamp"
        report = engine.validate(record)
        failed = [r.check_name for r in report.results if not r.passed]
        assert "valid_timestamp.event_timestamp" in failed

    def test_wrong_order_id_prefix_fails(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["order_id"] = "XYZ-12345"  # Should start with "ORD-"
        report = engine.validate(record)
        failed = [r.check_name for r in report.results if not r.passed]
        assert "format.order_id" in failed

    def test_quality_score_partial_failure(self):
        engine = self.DQEngine("orders")
        record = self._valid_order()
        record["currency"] = "INVALID"  # One low-severity failure
        report = engine.validate(record)
        assert 0.0 < report.quality_score < 1.0

    def test_batch_validation_summary(self):
        engine = self.DQEngine("orders")
        records = [self._valid_order() for _ in range(5)]
        records[0]["order_id"] = None  # 1 invalid
        with patch.object(engine, "_publish_cloudwatch_metrics"):
            with patch.object(engine, "_send_alert"):
                summary = engine.validate_batch(records)
        assert summary["total_records"] == 5
        assert summary["failed_records"] >= 1
        assert "pass_rate_pct" in summary
        assert "check_failure_counts" in summary

    def test_unknown_entity_raises(self):
        with pytest.raises(ValueError, match="Unknown entity"):
            self.DQEngine("nonexistent_entity")

    def test_payment_checks_available(self):
        engine = self.DQEngine("payments")
        assert len(engine.checks) > 0

    def test_clickstream_checks_available(self):
        engine = self.DQEngine("clickstream")
        assert len(engine.checks) > 0


# =============================================================
# Lambda Handler Tests (with moto mocking)
# =============================================================

class TestOrdersLambdaHandler:

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("ENVIRONMENT", "test")
        monkeypatch.setenv("SNS_ALERT_ARN", "")
        monkeypatch.setenv("DEDUP_TABLE", "test-dedup-orders")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

    def _encode_kinesis_record(self, payload: dict) -> str:
        import base64
        return base64.b64encode(json.dumps(payload).encode()).decode()

    def _valid_order_payload(self) -> dict:
        return {
            "event_id": "EVT-TEST123456",
            "event_type": "order_placed",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "order_id": "ORD-TESTABCDEF",
            "user_id": "USR-TEST001",
            "session_id": "SES-TESTABCDEF",
            "total_amount": 99.99,
            "subtotal": 89.99,
            "tax_amount": 7.20,
            "shipping_cost": 4.99,
            "discount_amount": 2.19,
            "currency": "USD",
            "country": "US",
            "payment_method": "credit_card",
            "order_status": "confirmed",
            "items": [{"product_id": "PRD-00001", "quantity": 1,
                       "unit_price": 89.99, "final_price": 89.99,
                       "product_name": "Test Product", "category": "Electronics",
                       "brand": "TestBrand", "discount_pct": 0}],
        }

    def test_empty_event_returns_200(self):
        """Lambda should handle empty Kinesis batch gracefully."""
        with patch("handler.write_to_s3_bronze"), \
             patch("handler.check_duplicate", return_value=False), \
             patch("handler.s3_client"), patch("handler.sns_client"), \
             patch("handler.dynamodb"):
            from handler import lambda_handler
            result = lambda_handler({"Records": []}, MagicMock(aws_request_id="test-req-id"))
        assert result["statusCode"] == 200
        assert result["total_records"] == 0

    def test_valid_order_processed(self):
        """Valid order should be written to S3 bronze."""
        payload = self._valid_order_payload()
        kinesis_event = {
            "Records": [{
                "kinesis": {"data": self._encode_kinesis_record(payload)},
                "eventSource": "aws:kinesis",
            }]
        }
        with patch("handler.write_to_s3_bronze", return_value="s3://test-bucket/bronze/...") as mock_write, \
             patch("handler.check_duplicate", return_value=False), \
             patch("handler.s3_client"), patch("handler.sns_client"), \
             patch("handler.dynamodb"):
            from handler import lambda_handler
            result = lambda_handler(kinesis_event, MagicMock(aws_request_id="test-req-id"))
        assert result["valid_records"] == 1
        assert result["failed_records"] == 0
        mock_write.assert_called_once()

    def test_duplicate_order_skipped(self):
        """Duplicate orders should be skipped, not failed."""
        payload = self._valid_order_payload()
        kinesis_event = {
            "Records": [{
                "kinesis": {"data": self._encode_kinesis_record(payload)},
            }]
        }
        with patch("handler.check_duplicate", return_value=True), \
             patch("handler.write_to_s3_bronze") as mock_write, \
             patch("handler.s3_client"), patch("handler.sns_client"), \
             patch("handler.dynamodb"):
            from handler import lambda_handler
            result = lambda_handler(kinesis_event, MagicMock(aws_request_id="test-req-id"))
        assert result["duplicate_records"] == 1
        assert result["valid_records"] == 0
        mock_write.assert_not_called()

    def test_invalid_json_handled(self):
        """Malformed JSON should be routed to DLQ."""
        import base64
        kinesis_event = {
            "Records": [{
                "kinesis": {"data": base64.b64encode(b"not-valid-json").decode()},
            }]
        }
        with patch("handler.send_to_dlq") as mock_dlq, \
             patch("handler.write_to_s3_bronze"), \
             patch("handler.s3_client"), patch("handler.sns_client"), \
             patch("handler.dynamodb"):
            from handler import lambda_handler
            result = lambda_handler(kinesis_event, MagicMock(aws_request_id="test-req-id"))
        assert result["failed_records"] == 1
        mock_dlq.assert_called_once()


# ─────────────────────────────────────────────
# Run directly
# ─────────────────────────────────────────────
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
