"""
AWS Glue ETL Job: Silver → Gold Layer
========================================
Reads cleaned Silver data and produces analytics-ready Gold tables
including business KPIs, aggregates, and fact/dimension tables
ready for loading into Snowflake.

Glue Version: 4.0 (PySpark 3.3 / Python 3.10)
Author: Data Engineering Team
Version: 1.0.0
"""

import sys
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, TimestampType
)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# Job Parameters
# ─────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_BUCKET",
        "ENVIRONMENT",
        "SILVER_PREFIX",
        "GOLD_PREFIX",
        "GLUE_DATABASE",
        "PROCESSING_DATE",
    ]
)

JOB_NAME        = args["JOB_NAME"]
S3_BUCKET       = args["S3_BUCKET"]
ENVIRONMENT     = args["ENVIRONMENT"]
SILVER_PREFIX   = args.get("SILVER_PREFIX", "silver")
GOLD_PREFIX     = args.get("GOLD_PREFIX", "gold")
GLUE_DATABASE   = args.get("GLUE_DATABASE", "ecommerce_catalog")
PROCESSING_DATE = args.get("PROCESSING_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ─────────────────────────────────────────────
# Spark / Glue Init
# ─────────────────────────────────────────────

sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session
job          = Job(glueContext)
job.init(JOB_NAME, args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")


# ─────────────────────────────────────────────
# Path Helpers
# ─────────────────────────────────────────────

def silver_path(entity: str) -> str:
    return f"s3://{S3_BUCKET}/{SILVER_PREFIX}/{entity}"

def gold_path(entity: str) -> str:
    return f"s3://{S3_BUCKET}/{GOLD_PREFIX}/{entity}"

def write_gold(df: DataFrame, name: str, partition_cols: Optional[List[str]] = None, mode: str = "overwrite") -> None:
    path = gold_path(name)
    logger.info(f"Writing Gold table [{name}] → {path} | rows={df.count():,}")
    writer = df.coalesce(8).write.mode(mode).option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info(f"Gold write complete: {name}")


# ─────────────────────────────────────────────
# 1. FACT TABLES
# ─────────────────────────────────────────────

def build_fact_orders(proc_date: str) -> DataFrame:
    """
    Build fact_orders: one row per order, business-ready.
    Includes all financial metrics, customer attributes, and derived KPIs.
    """
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = spark.read.parquet(silver_path("orders")).filter(
        (F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day)
    )

    # Explode items to calculate category-level metrics
    items_df = (
        df.select("order_id", F.explode("items").alias("item"))
        .select(
            "order_id",
            F.col("item.product_id").alias("top_product_id"),
            F.col("item.category").alias("top_category"),
            F.col("item.final_price").alias("item_price"),
            F.col("item.quantity").alias("item_qty"),
        )
        .groupBy("order_id")
        .agg(
            F.first("top_category").alias("primary_category"),
            F.count("top_product_id").alias("distinct_products"),
            F.sum(F.col("item_price") * F.col("item_qty")).alias("items_total"),
        )
    )

    fact = (
        df.join(items_df, "order_id", "left")
        .select(
            F.col("order_id"),
            F.col("event_id"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("device_type"),
            F.col("country"),
            F.col("currency"),
            F.col("payment_method"),
            F.col("order_status"),
            F.col("item_count"),
            F.col("total_amount").alias("order_total"),
            F.col("subtotal"),
            F.col("tax_amount"),
            F.col("shipping_cost"),
            F.col("discount_amount"),
            F.col("effective_discount_pct"),
            F.col("order_value_tier"),
            F.col("primary_category"),
            F.col("distinct_products"),
            F.col("customer_segment"),
            F.col("utm_source"),
            F.col("utm_medium"),
            F.col("is_prime"),
            F.col("is_gift"),
            F.col("is_weekend"),
            F.col("order_hour_bucket"),
            F.col("event_timestamp").alias("order_timestamp"),
            F.col("year"),
            F.col("month"),
            F.col("day"),
            F.col("hour"),
            F.current_timestamp().alias("gold_processed_at"),
        )
    )
    return fact


def build_fact_clickstream(proc_date: str) -> DataFrame:
    """Build fact_clickstream: enriched page-level event facts."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = spark.read.parquet(silver_path("clickstream")).filter(
        (F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day)
    )

    fact = df.select(
        "event_id", "session_id", "user_id",
        "page_type", "product_id", "category",
        "search_query", "time_on_page_seconds", "scroll_depth_pct",
        "load_time_ms", "is_bounce", "device_type", "country",
        "utm_source", "utm_medium", "customer_segment",
        "ab_test_variant", "viewport_width",
        F.col("event_timestamp").alias("view_timestamp"),
        "year", "month", "day", "hour",
        F.current_timestamp().alias("gold_processed_at"),
    )
    return fact


def build_fact_payments(proc_date: str) -> DataFrame:
    """Build fact_payments: payment transaction facts."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = spark.read.parquet(silver_path("payments")).filter(
        (F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day)
    )

    fact = df.select(
        "payment_id", "order_id", "user_id", "session_id",
        "payment_method", "payment_gateway", "payment_status",
        "amount", "currency", "failure_reason",
        "processing_time_ms", "is_3ds_authenticated",
        "gateway_response_code", "installment_plan",
        F.col("event_timestamp").alias("payment_timestamp"),
        "year", "month", "day", "hour",
        F.current_timestamp().alias("gold_processed_at"),
    )
    return fact


# ─────────────────────────────────────────────
# 2. DIMENSION TABLES
# ─────────────────────────────────────────────

def build_dim_users() -> DataFrame:
    """
    Build dim_users: latest known state per user.
    Uses window function to pick most recent record per user_id.
    """
    orders_df = spark.read.parquet(silver_path("orders")).select(
        "user_id", "country", "city", "device_type", "customer_segment",
        "is_prime", "event_timestamp"
    )
    clicks_df = spark.read.parquet(silver_path("clickstream")).select(
        "user_id", "country", "city", "device_type", "customer_segment",
        F.lit(None).cast(BooleanType()).alias("is_prime"), "event_timestamp"
    )

    combined = orders_df.unionByName(clicks_df)
    window = Window.partitionBy("user_id").orderBy(F.col("event_timestamp").desc())

    dim = (
        combined
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "event_timestamp")
        .withColumn("dim_user_sk", F.md5(F.col("user_id")))
        .withColumn("is_active", F.lit(True))
        .withColumn("updated_at", F.current_timestamp())
    )
    return dim


def build_dim_products() -> DataFrame:
    """
    Build dim_products: product catalog enriched from order line items.
    """
    df = (
        spark.read.parquet(silver_path("orders"))
        .select(F.explode("items").alias("item"))
        .select(
            F.col("item.product_id"),
            F.col("item.product_name"),
            F.col("item.category"),
            F.col("item.brand"),
            F.col("item.unit_price"),
        )
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.round(F.min("unit_price"), 2).alias("min_unit_price"),
            F.round(F.max("unit_price"), 2).alias("max_unit_price"),
        )
        .withColumn("dim_product_sk", F.md5(F.col("product_id")))
        .withColumn("is_active", F.lit(True))
        .withColumn("updated_at", F.current_timestamp())
    )
    return df


def build_dim_time() -> DataFrame:
    """
    Build dim_time: pre-generated date/time dimension for the next 3 years.
    """
    start_date = datetime(2024, 1, 1)
    end_date   = datetime(2027, 12, 31)
    date_range = []
    current    = start_date
    while current <= end_date:
        date_range.append((
            int(current.strftime("%Y%m%d")),  # date_key
            current.date().isoformat(),
            current.year,
            current.month,
            current.day,
            current.isocalendar()[1],          # week_of_year
            current.strftime("%A"),             # day_name
            current.strftime("%B"),             # month_name
            current.quarter if hasattr(current, "quarter") else (current.month - 1) // 3 + 1,
            current.weekday() >= 5,            # is_weekend
        ))
        current += timedelta(days=1)

    from pyspark.sql.types import (
        StructType, StructField, IntegerType, StringType, BooleanType
    )
    schema = StructType([
        StructField("date_key", IntegerType()),
        StructField("full_date", StringType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("day", IntegerType()),
        StructField("week_of_year", IntegerType()),
        StructField("day_name", StringType()),
        StructField("month_name", StringType()),
        StructField("quarter", IntegerType()),
        StructField("is_weekend", BooleanType()),
    ])
    return spark.createDataFrame(date_range, schema)


def build_dim_devices() -> DataFrame:
    """Build dim_devices: unique device/OS combinations."""
    df = (
        spark.read.parquet(silver_path("orders"))
        .select("device_type", "device_os", "browser")
        .distinct()
        .withColumn("device_sk", F.md5(F.concat_ws("|", "device_type", "device_os", "browser")))
        .withColumn("device_class",
            F.when(F.col("device_type") == "mobile", "smartphone")
             .when(F.col("device_type") == "tablet", "tablet")
             .otherwise("computer")
        )
        .withColumn("updated_at", F.current_timestamp())
    )
    return df


# ─────────────────────────────────────────────
# 3. GOLD AGGREGATES (KPI tables)
# ─────────────────────────────────────────────

def build_gold_daily_revenue(proc_date: str) -> DataFrame:
    """Daily revenue KPI table."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = spark.read.parquet(silver_path("orders")).filter(
        (F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day)
    )

    agg = (
        df.groupBy("year", "month", "day", "country", "device_type", "payment_method", "customer_segment")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("total_discounts"),
            F.round(F.sum("total_amount") - F.sum("discount_amount"), 2).alias("net_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.round(F.sum("shipping_cost"), 2).alias("total_shipping"),
            F.round(F.sum("tax_amount"), 2).alias("total_tax"),
        )
        .withColumn("revenue_per_customer", F.round(F.col("gross_revenue") / F.col("unique_customers"), 2))
        .withColumn("processed_at", F.current_timestamp())
    )
    return agg


def build_gold_product_performance(proc_date: str) -> DataFrame:
    """Product-level sales performance aggregations."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = (
        spark.read.parquet(silver_path("orders"))
        .filter((F.col("year") == dt.year) & (F.col("month") == dt.month))
        .select("order_id", "order_status", "year", "month", F.explode("items").alias("item"))
        .select(
            "order_id", "order_status", "year", "month",
            F.col("item.product_id"),
            F.col("item.product_name"),
            F.col("item.category"),
            F.col("item.brand"),
            F.col("item.quantity"),
            F.col("item.unit_price"),
            F.col("item.final_price"),
            F.col("item.discount_pct"),
        )
        .filter(F.col("order_status").isin(["confirmed", "shipped", "delivered"]))
    )

    agg = (
        df.groupBy("product_id", "product_name", "category", "brand", "year", "month")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.count("order_id").alias("order_count"),
            F.round(F.sum(F.col("final_price") * F.col("quantity")), 2).alias("total_revenue"),
            F.round(F.avg("final_price"), 2).alias("avg_selling_price"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        )
        .withColumn("revenue_rank",
            F.rank().over(Window.partitionBy("year", "month", "category").orderBy(F.col("total_revenue").desc()))
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    return agg


def build_gold_funnel_analytics(proc_date: str) -> DataFrame:
    """Conversion funnel: views → cart → order → payment."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")

    views = (
        spark.read.parquet(silver_path("clickstream"))
        .filter((F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day))
        .groupBy("year", "month", "day", "country", "device_type")
        .agg(F.countDistinct("session_id").alias("sessions"), F.count("event_id").alias("page_views"))
    )

    carts = (
        spark.read.parquet(silver_path("carts"))
        .filter((F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day))
        .filter(F.col("action") == "item_added")
        .groupBy("year", "month", "day", "country", "device_type")
        .agg(F.countDistinct("session_id").alias("cart_sessions"))
    )

    orders = (
        spark.read.parquet(silver_path("orders"))
        .filter((F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day))
        .groupBy("year", "month", "day", "country", "device_type")
        .agg(F.countDistinct("session_id").alias("order_sessions"), F.sum("total_amount").alias("revenue"))
    )

    funnel = (
        views
        .join(carts, ["year", "month", "day", "country", "device_type"], "left")
        .join(orders, ["year", "month", "day", "country", "device_type"], "left")
        .fillna(0, subset=["cart_sessions", "order_sessions"])
        .withColumn("cart_rate", F.round(F.col("cart_sessions") / F.col("sessions"), 4))
        .withColumn("conversion_rate", F.round(F.col("order_sessions") / F.col("sessions"), 4))
        .withColumn("cart_to_order_rate",
            F.when(F.col("cart_sessions") > 0,
                F.round(F.col("order_sessions") / F.col("cart_sessions"), 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    return funnel


def build_gold_hourly_revenue(proc_date: str) -> DataFrame:
    """Hourly revenue for real-time dashboard."""
    dt = datetime.strptime(proc_date, "%Y-%m-%d")
    df = (
        spark.read.parquet(silver_path("orders"))
        .filter((F.col("year") == dt.year) & (F.col("month") == dt.month) & (F.col("day") == dt.day))
        .filter(F.col("order_status").isin(["confirmed", "shipped", "delivered"]))
    )

    agg = (
        df.groupBy("year", "month", "day", "hour", "country")
        .agg(
            F.count("order_id").alias("orders"),
            F.countDistinct("user_id").alias("buyers"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("total_amount"), 2).alias("aov"),
        )
        .withColumn("revenue_cumulative",
            F.sum("revenue").over(
                Window.partitionBy("year", "month", "day", "country")
                      .orderBy("hour")
                      .rowsBetween(Window.unboundedPreceding, 0)
            )
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    return agg


# ─────────────────────────────────────────────
# Main Execution
# ─────────────────────────────────────────────

def run():
    logger.info(f"Starting Silver → Gold ETL | date={PROCESSING_DATE} | env={ENVIRONMENT}")
    start = datetime.now(timezone.utc)

    # ── Fact Tables ──────────────────────────────
    logger.info("Building fact tables...")
    write_gold(build_fact_orders(PROCESSING_DATE), "fact_orders", ["year", "month", "day"])
    write_gold(build_fact_payments(PROCESSING_DATE), "fact_payments", ["year", "month", "day"])
    write_gold(build_fact_clickstream(PROCESSING_DATE), "fact_clickstream", ["year", "month", "day"])

    # ── Dimension Tables ─────────────────────────
    logger.info("Building dimension tables...")
    write_gold(build_dim_users(), "dim_users")
    write_gold(build_dim_products(), "dim_products")
    write_gold(build_dim_time(), "dim_time")
    write_gold(build_dim_devices(), "dim_devices")

    # ── KPI / Aggregate Tables ───────────────────
    logger.info("Building KPI aggregates...")
    write_gold(build_gold_daily_revenue(PROCESSING_DATE), "agg_daily_revenue", ["year", "month", "day"])
    write_gold(build_gold_product_performance(PROCESSING_DATE), "agg_product_performance", ["year", "month"])
    write_gold(build_gold_funnel_analytics(PROCESSING_DATE), "agg_funnel", ["year", "month", "day"])
    write_gold(build_gold_hourly_revenue(PROCESSING_DATE), "agg_hourly_revenue", ["year", "month", "day"])

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"Gold layer complete | elapsed={elapsed:.1f}s")


run()
job.commit()
