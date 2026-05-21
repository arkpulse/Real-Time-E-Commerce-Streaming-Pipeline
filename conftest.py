"""
conftest.py — shared pytest fixtures
"""

import os
import sys
import pytest

# Make source modules importable from tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "producer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "data_quality"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lambda/orders_processor"))


@pytest.fixture(autouse=True)
def aws_mock_env(monkeypatch):
    """Set dummy AWS env vars so boto3 doesn't look for real creds in unit tests."""
    monkeypatch.setenv("AWS_DEFAULT_REGION",    "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID",     "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN",    "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN",     "testing")
