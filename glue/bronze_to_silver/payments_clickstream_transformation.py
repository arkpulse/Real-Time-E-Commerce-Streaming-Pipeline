"""
AWS Glue ETL Job: Bronze → Silver — Payments & Clickstream
============================================================
Transforms raw payments and clickstream events from Bronze NDJSON
into clean, typed, deduplicated Silver Parquet tables.

Glue Version: 4.0  |  PySpark 3.3  |  Python 3.10
Author: Data Engineering Team
Version: 1.0.0
"""

import sys
import logging
from datetime import datetime, timezone
from typing import List, Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────
# Job Parameters
# ─────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "S3_BUCKET", "ENVIRONMENT",
    "BRONZE_PREFIX", "SILVER_PREFIX",
    "GLUE_DATABASE", "PROCESSING_DATE", "INCREMENTAL_MODE",
])

JOB_NAME        = args["JOB_NAME"]
S3_BUCKET       = args["S3_BUCKET"]
ENVIRONMENT     = args["ENVIRONMENT"]
BRONZE_PREFIX   = args.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX   = args.get("SILVER_PREFIX", "silver")
GLUE_DATABASE   = args.get("GLUE_DATABASE", "ecommerce_catalog")
INCREMENTAL_MODE = args.get("INCREMENTAL_MODE", "true").lower() == "true"
PROCESSING_DATE  = args.get("PROCESSING_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(JOB_NAME, args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def bronze_path(entity: str, date_str: Optional[str] = None) -> str:
    base = f"s3://{S3_BUCKET}/{BRONZE_PREFIX}/{entity}"
    if date_str and INCREMENTAL_MODE:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{base}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    return base

def silver_path(entity: str) -> str:
    return f"s3://{S3_BUCKET}/{SILVER_PREFIX}/{entity}"

def deduplicate(df: DataFrame, partition_cols: List[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

def write_silver(df: DataFrame, entity: str, partition_cols: List[str]) -> None:
    path = silver_path(entity)
    logger.info(f"Writing silver/{entity} → {path} | rows={df.count():,}")
    (
        df.repartition(*[F.col(c) for c in partition_cols])
        .write.mode("append")
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 500_000)
        .parquet(path)
    )
    logger.info(f"Silver write complete: {entity}")


# ─────────────────────────────────────────────
# Payments Transformation
# ─────────────────────────────────────────────

def transform_payments(path: str) -> DataFrame:
    logger.info(f"Reading payments bronze: {path}")
    df_raw = spark.read.option("multiline", "false").json(path)
    if df_raw.count() == 0:
        return spark.createDataFrame([], StructType([]))

    df = (
        df_raw
        .withColumn("event_timestamp",     F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("amount",              F.col("amount").cast(DoubleType()))
        .withColumn("processing_time_ms",  F.col("processing_time_ms").cast(IntegerType()))
        .withColumn("installment_plan",    F.col("installment_plan").cast(IntegerType()))
        .withColumn("is_3ds_authenticated",F.col("is_3ds_authenticated").cast(BooleanType()))
        .withColumn("is_fraud_flagged",    F.col("is_fraud_flagged").cast(BooleanType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    # Drop records missing primary keys
    df = df.filter(
        F.col("payment_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") > 0) &
        (F.col("amount") < 100_000)
    )

    # Deduplicate by payment_id
    df = deduplicate(df, ["payment_id"], "event_timestamp")

    # Derived columns
    df = (
        df
        .withColumn("is_successful",
            (F.col("payment_status") == "success").cast(BooleanType()))
        .withColumn("amount_tier",
            F.when(F.col("amount") < 50,   "micro")
             .when(F.col("amount") < 200,  "small")
             .when(F.col("amount") < 1000, "medium")
             .otherwise("large"))
        .withColumn("payment_hour_bucket",
            F.when(F.col("hour").between(0,  5),  "late_night")
             .when(F.col("hour").between(6,  11), "morning")
             .when(F.col("hour").between(12, 17), "afternoon")
             .when(F.col("hour").between(18, 21), "evening")
             .otherwise("night"))
        .withColumn("is_high_risk",
            (F.col("is_fraud_flagged") |
             (F.col("processing_time_ms") > 5000) |
             (F.col("amount") > 5000)).cast(BooleanType()))
        .withColumn("data_quality_score",
            F.when(
                F.col("order_id").isNotNull() &
                F.col("user_id").isNotNull() &
                F.col("payment_gateway").isNotNull(), "HIGH")
            .otherwise("MEDIUM"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    logger.info(f"Payments silver ready: {df.count():,} rows")
    return df


# ─────────────────────────────────────────────
# Clickstream Transformation
# ─────────────────────────────────────────────

def transform_clickstream(path: str) -> DataFrame:
    logger.info(f"Reading clickstream bronze: {path}")
    df_raw = spark.read.option("multiline", "false").json(path)
    if df_raw.count() == 0:
        return spark.createDataFrame([], StructType([]))

    df = (
        df_raw
        .withColumn("event_timestamp",     F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("time_on_page_seconds",F.col("time_on_page_seconds").cast(IntegerType()))
        .withColumn("scroll_depth_pct",    F.col("scroll_depth_pct").cast(IntegerType()))
        .withColumn("load_time_ms",        F.col("load_time_ms").cast(IntegerType()))
        .withColumn("viewport_width",      F.col("viewport_width").cast(IntegerType()))
        .withColumn("search_result_count", F.col("search_result_count").cast(IntegerType()))
        .withColumn("is_bounce",           F.col("is_bounce").cast(BooleanType()))
        .withColumn("is_authenticated",    F.col("is_authenticated").cast(BooleanType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    # Drop records without session identifiers
    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("session_id").isNotNull()
    )

    # Clamp time_on_page to sane range (0–3600s)
    df = df.withColumn("time_on_page_seconds",
        F.when(F.col("time_on_page_seconds").between(0, 3600),
               F.col("time_on_page_seconds"))
        .otherwise(F.lit(None).cast(IntegerType())))

    # Deduplicate by event_id
    df = deduplicate(df, ["event_id"], "event_timestamp")

    # Derived columns
    df = (
        df
        .withColumn("engagement_level",
            F.when(
                (F.col("time_on_page_seconds") > 120) &
                (F.col("scroll_depth_pct") > 70) &
                (~F.col("is_bounce")), "high")
            .when(
                (F.col("time_on_page_seconds") > 30) &
                (F.col("scroll_depth_pct") > 40), "medium")
            .otherwise("low"))
        .withColumn("is_slow_page",
            (F.col("load_time_ms") > 3000).cast(BooleanType()))
        .withColumn("is_mobile",
            (F.col("device_type") == "mobile").cast(BooleanType()))
        .withColumn("page_category",
            F.when(F.col("page_type").isin("home", "category"),          "browsing")
             .when(F.col("page_type") == "product_detail",               "product")
             .when(F.col("page_type") == "search_results",               "search")
             .when(F.col("page_type").isin("cart", "checkout"),          "purchase_funnel")
             .when(F.col("page_type") == "order_confirmation",           "post_purchase")
             .otherwise("account"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    logger.info(f"Clickstream silver ready: {df.count():,} rows")
    return df


# ─────────────────────────────────────────────
# Cart Transformation
# ─────────────────────────────────────────────

def transform_carts(path: str) -> DataFrame:
    logger.info(f"Reading carts bronze: {path}")
    df_raw = spark.read.option("multiline", "false").json(path)
    if df_raw.count() == 0:
        return spark.createDataFrame([], StructType([]))

    df = (
        df_raw
        .withColumn("event_timestamp",    F.to_timestamp("event_timestamp"))
        .withColumn("unit_price",         F.col("unit_price").cast(DoubleType()))
        .withColumn("quantity",           F.col("quantity").cast(IntegerType()))
        .withColumn("cart_total",         F.col("cart_total").cast(DoubleType()))
        .withColumn("cart_item_count",    F.col("cart_item_count").cast(IntegerType()))
        .withColumn("is_wishlist_item",   F.col("is_wishlist_item").cast(BooleanType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("session_id").isNotNull() &
        F.col("product_id").isNotNull()
    )

    df = deduplicate(df, ["event_id"], "event_timestamp")

    df = (
        df
        .withColumn("item_total_value",
            F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("is_abandonment",
            F.col("action").isin("cart_abandoned").cast(BooleanType()))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    logger.info(f"Carts silver ready: {df.count():,} rows")
    return df


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def run():
    logger.info(f"Bronze→Silver ETL | date={PROCESSING_DATE} | env={ENVIRONMENT}")
    start = datetime.now(timezone.utc)

    for entity, transform_fn in [
        ("payments",    transform_payments),
        ("clickstream", transform_clickstream),
        ("carts",       transform_carts),
    ]:
        try:
            path = bronze_path(entity, PROCESSING_DATE)
            df   = transform_fn(path)
            if df.columns:  # non-empty schema
                write_silver(df, entity, ["year", "month", "day"])
        except Exception as e:
            logger.error(f"Failed to transform {entity}: {e}", exc_info=True)
            raise

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"ETL complete | elapsed={elapsed:.1f}s")


run()
job.commit()
