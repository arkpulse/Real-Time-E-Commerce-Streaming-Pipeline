"""
Real-Time E-Commerce Event Generator
=====================================
Simulates a production e-commerce platform generating streaming events
across multiple event types: orders, payments, clickstream, and cart activity.

Author: Abdul Rehman Khalid
Version: 1.0.0
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from faker import Faker

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("EventGenerator")

fake = Faker()

# ─────────────────────────────────────────────
# Static Reference Data
# ─────────────────────────────────────────────

PRODUCT_CATALOG: List[Dict] = [
    {"product_id": f"PRD-{str(i).zfill(5)}", "name": fake.catch_phrase(),
     "category": random.choice(["Electronics", "Fashion", "Home", "Sports", "Beauty", "Books", "Toys", "Food"]),
     "price": round(random.uniform(5.0, 1500.0), 2),
     "brand": fake.company()}
    for i in range(1, 501)
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_OS = {
    "mobile": ["iOS 17", "Android 14", "iOS 16"],
    "desktop": ["Windows 11", "macOS Ventura", "Ubuntu 22.04", "Windows 10"],
    "tablet": ["iPadOS 17", "Android 13"],
}
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer", "crypto"]
ORDER_STATUSES = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"]
COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "IN", "BR", "MX", "SG", "NL", "SE", "NO", "AE"]
PAGE_TYPES = ["home", "category", "product_detail", "search_results", "cart", "checkout", "order_confirmation", "account", "wishlist"]
SEARCH_TERMS = ["laptop", "shoes", "shirt", "headphones", "phone", "camera", "watch", "bag", "book", "game"]
UTM_SOURCES = ["google", "facebook", "instagram", "email", "direct", "organic", "twitter", "affiliate"]
UTM_MEDIUMS = ["cpc", "social", "email", "organic", "referral", "display"]


class EventGenerator:
    """
    Core event generator that produces realistic e-commerce streaming events.
    Maintains session state to ensure coherent user journeys.
    """

    def __init__(self):
        self.active_sessions: Dict[str, Dict] = {}
        self.max_sessions = int(os.getenv("MAX_CONCURRENT_SESSIONS", "200"))
        self.product_catalog = PRODUCT_CATALOG

    def _get_or_create_session(self) -> Dict:
        """Retrieve an existing session or create a new user session."""
        if self.active_sessions and random.random() > 0.3:
            session_key = random.choice(list(self.active_sessions.keys()))
            return self.active_sessions[session_key]

        session_id = f"SES-{uuid.uuid4().hex[:12].upper()}"
        user_id = f"USR-{uuid.uuid4().hex[:8].upper()}"
        device = random.choice(DEVICES)
        session = {
            "session_id": session_id,
            "user_id": user_id,
            "device_type": device,
            "device_os": random.choice(DEVICE_OS[device]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "country": random.choice(COUNTRIES),
            "city": fake.city(),
            "ip_address": fake.ipv4_public(),
            "utm_source": random.choice(UTM_SOURCES),
            "utm_medium": random.choice(UTM_MEDIUMS),
            "utm_campaign": f"campaign_{random.randint(1, 50)}",
            "session_start": datetime.now(timezone.utc).isoformat(),
            "cart_items": [],
            "is_authenticated": random.random() > 0.3,
            "customer_segment": random.choice(["new", "returning", "vip", "churned"]),
        }

        if len(self.active_sessions) >= self.max_sessions:
            oldest_key = next(iter(self.active_sessions))
            del self.active_sessions[oldest_key]

        self.active_sessions[session_id] = session
        return session

    def _base_event_fields(self, session: Dict, event_type: str) -> Dict:
        """Generate common fields present in every event."""
        return {
            "event_id": f"EVT-{uuid.uuid4().hex.upper()}",
            "event_type": event_type,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "ingestion_timestamp": None,  # Set by Lambda
            "schema_version": "1.0.0",
            "platform": "web" if session["device_type"] == "desktop" else "app",
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "device_type": session["device_type"],
            "device_os": session["device_os"],
            "browser": session["browser"],
            "country": session["country"],
            "city": session["city"],
            "ip_address": session["ip_address"],
            "utm_source": session["utm_source"],
            "utm_medium": session["utm_medium"],
            "utm_campaign": session["utm_campaign"],
            "is_authenticated": session["is_authenticated"],
            "customer_segment": session["customer_segment"],
        }

    # ──────────────────────────────────────────
    # Event Factory Methods
    # ──────────────────────────────────────────

    def generate_order_event(self) -> Dict[str, Any]:
        """Generate a realistic order placement event."""
        session = self._get_or_create_session()
        items = random.sample(self.product_catalog, k=random.randint(1, 6))
        cart_items = [
            {
                "product_id": item["product_id"],
                "product_name": item["name"],
                "category": item["category"],
                "brand": item["brand"],
                "quantity": random.randint(1, 5),
                "unit_price": item["price"],
                "discount_pct": random.choice([0, 5, 10, 15, 20, 25]),
                "final_price": round(item["price"] * random.uniform(0.75, 1.0), 2),
            }
            for item in items
        ]
        subtotal = sum(i["final_price"] * i["quantity"] for i in cart_items)
        tax = round(subtotal * 0.08, 2)
        shipping = round(random.choice([0, 4.99, 9.99, 14.99, 19.99]), 2)
        total = round(subtotal + tax + shipping, 2)

        order = {
            **self._base_event_fields(session, "order_placed"),
            "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
            "order_status": random.choice(["pending", "confirmed"]),
            "payment_method": random.choice(PAYMENT_METHODS),
            "currency": "USD",
            "subtotal": round(subtotal, 2),
            "tax_amount": tax,
            "shipping_cost": shipping,
            "total_amount": total,
            "discount_amount": round(subtotal - (total - tax - shipping), 2),
            "coupon_code": f"SAVE{random.randint(10, 30)}" if random.random() > 0.6 else None,
            "items": cart_items,
            "item_count": len(cart_items),
            "shipping_address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.zipcode(),
                "country": session["country"],
            },
            "estimated_delivery_days": random.randint(2, 10),
            "is_prime": random.random() > 0.5,
            "is_gift": random.random() > 0.85,
        }
        logger.debug(f"Generated order event: {order['order_id']}")
        return order

    def generate_payment_event(self) -> Dict[str, Any]:
        """Generate a payment processing event."""
        session = self._get_or_create_session()
        amount = round(random.uniform(10.0, 2500.0), 2)
        is_success = random.random() > 0.06  # 94% success rate

        payment = {
            **self._base_event_fields(session, "payment_processed"),
            "payment_id": f"PAY-{uuid.uuid4().hex[:10].upper()}",
            "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_gateway": random.choice(["stripe", "paypal", "braintree", "adyen", "square"]),
            "currency": "USD",
            "amount": amount,
            "payment_status": "success" if is_success else random.choice(["failed", "declined", "pending", "refunded"]),
            "failure_reason": None if is_success else random.choice([
                "insufficient_funds", "card_expired", "fraud_detected",
                "bank_decline", "invalid_cvv", "address_mismatch"
            ]),
            "transaction_id": f"TXN-{uuid.uuid4().hex[:14].upper()}",
            "gateway_response_code": "00" if is_success else random.choice(["05", "14", "51", "54"]),
            "processing_time_ms": random.randint(120, 2800),
            "is_3ds_authenticated": random.random() > 0.4,
            "card_last_four": str(random.randint(1000, 9999)) if "card" in random.choice(PAYMENT_METHODS) else None,
            "installment_plan": random.choice([1, 3, 6, 12]) if random.random() > 0.7 else 1,
        }
        logger.debug(f"Generated payment event: {payment['payment_id']}")
        return payment

    def generate_clickstream_event(self) -> Dict[str, Any]:
        """Generate a user clickstream/navigation event."""
        session = self._get_or_create_session()
        page_type = random.choice(PAGE_TYPES)
        product = random.choice(self.product_catalog) if page_type == "product_detail" else None

        click = {
            **self._base_event_fields(session, "page_view"),
            "page_type": page_type,
            "page_url": f"https://shop.example.com/{page_type.replace('_', '/')}",
            "referrer_url": f"https://shop.example.com/{random.choice(PAGE_TYPES).replace('_', '/')}",
            "product_id": product["product_id"] if product else None,
            "product_name": product["name"] if product else None,
            "category": product["category"] if product else (
                random.choice(["Electronics", "Fashion", "Home", "Sports"]) if page_type == "category" else None
            ),
            "search_query": random.choice(SEARCH_TERMS) if page_type == "search_results" else None,
            "search_result_count": random.randint(0, 500) if page_type == "search_results" else None,
            "time_on_page_seconds": random.randint(3, 480),
            "scroll_depth_pct": random.randint(10, 100),
            "click_x": random.randint(0, 1920),
            "click_y": random.randint(0, 1080),
            "is_bounce": random.random() > 0.65,
            "viewport_width": random.choice([375, 768, 1280, 1440, 1920]),
            "load_time_ms": random.randint(200, 4000),
            "ab_test_variant": random.choice(["control", "variant_a", "variant_b"]),
        }
        logger.debug(f"Generated clickstream event: {click['event_id']}")
        return click

    def generate_cart_event(self) -> Dict[str, Any]:
        """Generate a shopping cart activity event."""
        session = self._get_or_create_session()
        product = random.choice(self.product_catalog)
        action = random.choice(["item_added", "item_removed", "quantity_updated", "cart_viewed", "cart_abandoned", "cart_saved"])

        cart = {
            **self._base_event_fields(session, f"cart_{action}"),
            "cart_id": f"CRT-{session['session_id'][-8:]}",
            "action": action,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
            "brand": product["brand"],
            "unit_price": product["price"],
            "quantity": random.randint(1, 10),
            "cart_total": round(random.uniform(20.0, 1200.0), 2),
            "cart_item_count": random.randint(1, 12),
            "is_wishlist_item": random.random() > 0.7,
            "recommendation_source": random.choice(["organic", "recommended", "upsell", "cross_sell", None]),
            "time_since_add_minutes": random.randint(0, 4320) if action == "cart_abandoned" else None,
            "abandonment_step": random.choice(["cart", "shipping", "payment"]) if action == "cart_abandoned" else None,
        }
        logger.debug(f"Generated cart event: {cart['event_id']}")
        return cart


def get_generator() -> EventGenerator:
    """Singleton generator instance."""
    if not hasattr(get_generator, "_instance"):
        get_generator._instance = EventGenerator()
    return get_generator._instance
