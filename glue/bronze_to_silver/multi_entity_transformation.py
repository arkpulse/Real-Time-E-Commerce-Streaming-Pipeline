"""
AWS Glue ETL: Bronze → Silver for Payments & Clickstream
==========================================================
Companion transformation scripts for payments and clickstream streams.

Author: Data Engineering Team
"""

import sys
import logging
from datetime import datetime, timezone
from typing import List

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, TimestampType
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "S3_BUCKET", "ENVIRONMENT",
    "BRONZE_PREFIX", "SILVER_PREFIX", "PROCESSING_DATE",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET       = args["S3_BUCKET"]
BRONZE_PREFIX   = args.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX   = args.get("SILVER_PREFIX", "silver")
PROCESSING_DATE = args.get("PROCESSING_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")


def bronze_path(entity: str) -> str:
    dt = datetime.strptime(PROCESSING_DATE, "%Y-%m-%d")
    return (f"s3://{S3_BUCKET}/{BRONZE_PREFIX}/{entity}/"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}")


def silver_path(entity: str) -> str:
    return f"s3://{S3_BUCKET}/{SILVER_PREFIX}/{entity}"


def write_silver(df: DataFrame, entity: str, partitions: List[str]) -> None:
    count = df.count()
    logger.info(f"Writing {entity} silver | rows={count:,}")
    (df.repartition(*[F.col(c) for c in partitions])
       .write
       .mode("append")
       .partitionBy(*partitions)
       .option("compression", "snappy")
       .parquet(silver_path(entity)))
    logger.info(f"Silver write complete: {entity}")


def deduplicate(df: DataFrame, partition_cols: List[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


# ─────────────────────────────────────────────
# PAYMENTS TRANSFORMATION
# ─────────────────────────────────────────────

def transform_payments() -> DataFrame:
    path = bronze_path("payments")
    logger.info(f"Reading payments bronze: {path}")

    try:
        df = spark.read.option("multiline", "false").json(path)
    except Exception as e:
        logger.warning(f"No payments data for {PROCESSING_DATE}: {e}")
        return spark.createDataFrame([], schema="payment_id STRING")

    if df.count() == 0:
        logger.info("Empty payments bronze — skipping")
        return df

    df = (
        df
        .filter(F.col("payment_id").isNotNull() & F.col("amount").isNotNull())
        .withColumn("event_timestamp",      F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp",  F.to_timestamp("ingestion_timestamp"))
        .withColumn("amount",               F.col("amount").cast(DoubleType()))
        .withColumn("processing_time_ms",   F.col("processing_time_ms").cast(IntegerType()))
        .withColumn("is_3ds_authenticated", F.col("is_3ds_authenticated").cast(BooleanType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    # Filter invalid amounts
    df = df.filter((F.col("amount") > 0) & (F.col("amount") < 100000))

    # Dedup by payment_id
    df = deduplicate(df, ["payment_id"], "event_timestamp")

    # Enrich
    df = (
        df
        .withColumn("is_successful",    F.col("payment_status") == "success")
        .withColumn("is_failed",        F.col("payment_status").isin(["failed", "declined"]))
        .withColumn("processing_tier",
            F.when(F.col("processing_time_ms") < 500,  "fast")
             .when(F.col("processing_time_ms") < 1500, "normal")
             .otherwise("slow"))
        .withColumn("amount_tier",
            F.when(F.col("amount") < 50,    "micro")
             .when(F.col("amount") < 200,   "small")
             .when(F.col("amount") < 1000,  "medium")
             .otherwise("large"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    logger.info(f"Payments silver ready | rows={df.count():,}")
    return df


# ─────────────────────────────────────────────
# CLICKSTREAM TRANSFORMATION
# ─────────────────────────────────────────────

def transform_clickstream() -> DataFrame:
    path = bronze_path("clickstream")
    logger.info(f"Reading clickstream bronze: {path}")

    try:
        df = spark.read.option("multiline", "false").json(path)
    except Exception as e:
        logger.warning(f"No clickstream data for {PROCESSING_DATE}: {e}")
        return spark.createDataFrame([], schema="event_id STRING")

    if df.count() == 0:
        logger.info("Empty clickstream bronze — skipping")
        return df

    df = (
        df
        .filter(F.col("event_id").isNotNull() & F.col("session_id").isNotNull())
        .withColumn("event_timestamp",     F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("time_on_page_seconds", F.col("time_on_page_seconds").cast(IntegerType()))
        .withColumn("scroll_depth_pct",    F.col("scroll_depth_pct").cast(IntegerType()))
        .withColumn("load_time_ms",        F.col("load_time_ms").cast(IntegerType()))
        .withColumn("is_bounce",           F.col("is_bounce").cast(BooleanType()))
        .withColumn("viewport_width",      F.col("viewport_width").cast(IntegerType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    # Clamp unrealistic values
    df = df.withColumn("time_on_page_seconds",
                       F.when(F.col("time_on_page_seconds") > 3600, 3600)
                        .otherwise(F.col("time_on_page_seconds")))

    # Dedup (clickstream events can arrive once per event_id)
    df = deduplicate(df, ["event_id"], "event_timestamp")

    # Enrich
    df = (
        df
        .withColumn("engagement_score",
            F.when(
                (F.col("time_on_page_seconds") > 60) &
                (F.col("scroll_depth_pct") > 50) &
                (~F.col("is_bounce")), "high"
            ).when(
                (F.col("time_on_page_seconds") > 20) |
                (F.col("scroll_depth_pct") > 25), "medium"
            ).otherwise("low"))
        .withColumn("is_product_page",
                    F.col("page_type") == "product_detail")
        .withColumn("is_checkout_page",
                    F.col("page_type") == "checkout")
        .withColumn("is_search",
                    F.col("page_type") == "search_results")
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    logger.info(f"Clickstream silver ready | rows={df.count():,}")
    return df


# ─────────────────────────────────────────────
# CART TRANSFORMATION
# ─────────────────────────────────────────────

def transform_carts() -> DataFrame:
    path = bronze_path("carts")
    logger.info(f"Reading carts bronze: {path}")

    try:
        df = spark.read.option("multiline", "false").json(path)
    except Exception as e:
        logger.warning(f"No carts data for {PROCESSING_DATE}: {e}")
        return spark.createDataFrame([], schema="event_id STRING")

    if df.count() == 0:
        return df

    df = (
        df
        .filter(F.col("event_id").isNotNull())
        .withColumn("event_timestamp",     F.to_timestamp("event_timestamp"))
        .withColumn("unit_price",  F.col("unit_price").cast(DoubleType()))
        .withColumn("cart_total",  F.col("cart_total").cast(DoubleType()))
        .withColumn("quantity",    F.col("quantity").cast(IntegerType()))
        .withColumn("year",  F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day",   F.col("day").cast(IntegerType()))
        .withColumn("hour",  F.col("hour").cast(IntegerType()))
    )

    df = deduplicate(df, ["event_id"], "event_timestamp")

    df = (
        df
        .withColumn("is_abandonment",  F.col("action") == "cart_abandoned")
        .withColumn("is_add",          F.col("action") == "item_added")
        .withColumn("is_remove",       F.col("action") == "item_removed")
        .withColumn("line_value",
                    F.round(F.col("unit_price") * F.col("quantity"), 2))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    return df


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run():
    logger.info(f"Starting multi-entity Bronze→Silver | date={PROCESSING_DATE}")
    start = datetime.now(timezone.utc)

    try:
        payments_df = transform_payments()
        if payments_df.count() > 0:
            write_silver(payments_df, "payments", ["year", "month", "day"])
    except Exception as e:
        logger.error(f"Payments transform failed: {e}", exc_info=True)

    try:
        clicks_df = transform_clickstream()
        if clicks_df.count() > 0:
            write_silver(clicks_df, "clickstream", ["year", "month", "day"])
    except Exception as e:
        logger.error(f"Clickstream transform failed: {e}", exc_info=True)

    try:
        carts_df = transform_carts()
        if carts_df.count() > 0:
            write_silver(carts_df, "carts", ["year", "month", "day"])
    except Exception as e:
        logger.error(f"Carts transform failed: {e}", exc_info=True)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"Multi-entity Silver complete | elapsed={elapsed:.1f}s")


run()
job.commit()
