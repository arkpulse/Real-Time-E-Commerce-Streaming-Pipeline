"""
AWS Glue ETL Job: Bronze → Silver Layer
==========================================
Reads raw event data from S3 Bronze zone, applies transformations,
schema standardization, deduplication, and null handling,
then writes clean data to Silver zone in partitioned Parquet format.

Glue Version: 4.0 (PySpark 3.3 / Python 3.10)
Author: Data Engineering Team
Version: 1.0.0
"""

import sys
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    LongType, MapType, StringType, StructField,
    StructType, TimestampType,
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
        "BRONZE_PREFIX",
        "SILVER_PREFIX",
        "GLUE_DATABASE",
        "PROCESSING_DATE",       # Optional: YYYY-MM-DD for backfill
        "INCREMENTAL_MODE",       # "true" / "false"
    ]
)

JOB_NAME = args["JOB_NAME"]
S3_BUCKET = args["S3_BUCKET"]
ENVIRONMENT = args["ENVIRONMENT"]
BRONZE_PREFIX = args.get("BRONZE_PREFIX", "bronze")
SILVER_PREFIX = args.get("SILVER_PREFIX", "silver")
GLUE_DATABASE = args.get("GLUE_DATABASE", "ecommerce_catalog")
INCREMENTAL_MODE = args.get("INCREMENTAL_MODE", "true").lower() == "true"
PROCESSING_DATE = args.get("PROCESSING_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

# ─────────────────────────────────────────────
# Spark / Glue Initialization
# ─────────────────────────────────────────────

sc = SparkContext()
sc.setLogLevel("WARN")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Optimize Spark for S3 and Parquet
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.speculation", "false")


# ─────────────────────────────────────────────
# Schema Definitions (Silver Layer)
# ─────────────────────────────────────────────

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("final_price", DoubleType(), True),
])

ORDERS_SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("processing_id", StringType(), True),
    StructField("order_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_os", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("subtotal", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("shipping_cost", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("coupon_code", StringType(), True),
    StructField("is_prime", BooleanType(), True),
    StructField("is_gift", BooleanType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
])


# ─────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────

def get_bronze_path(entity: str, date_str: Optional[str] = None) -> str:
    """Build S3 path for Bronze zone with optional date partition."""
    base = f"s3://{S3_BUCKET}/{BRONZE_PREFIX}/{entity}"
    if date_str and INCREMENTAL_MODE:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{base}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    return base


def get_silver_path(entity: str) -> str:
    """Build S3 path for Silver zone."""
    return f"s3://{S3_BUCKET}/{SILVER_PREFIX}/{entity}"


def log_dataframe_stats(df: DataFrame, stage: str, name: str) -> None:
    """Log record count and sample stats at each transformation stage."""
    count = df.count()
    logger.info(f"[{stage}] {name}: {count:,} records")


def standardize_nulls(df: DataFrame, columns: List[str], fill_value: str = "UNKNOWN") -> DataFrame:
    """Replace nulls in string columns with a standard value."""
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.when(F.col(col).isNull() | (F.col(col) == ""), fill_value).otherwise(F.col(col)))
    return df


def deduplicate(df: DataFrame, partition_cols: List[str], order_col: str) -> DataFrame:
    """
    Keep the most recent record per partition key using window function.
    Handles late-arriving data gracefully.
    """
    window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ─────────────────────────────────────────────
# Orders Transformation
# ─────────────────────────────────────────────

def transform_orders(bronze_path: str) -> DataFrame:
    """
    Full Bronze → Silver transformation for orders.
    Steps: read → cast types → null handling → dedup → enrich → validate
    """
    logger.info(f"Reading orders bronze data from: {bronze_path}")

    # Read NDJSON from Bronze
    df_raw = spark.read.option("multiline", "false").json(bronze_path)
    log_dataframe_stats(df_raw, "BRONZE_READ", "orders")

    if df_raw.count() == 0:
        logger.info("No data found in bronze path — skipping")
        return spark.createDataFrame([], ORDERS_SILVER_SCHEMA)

    # ── Cast and standardize types ──────────────
    df = (
        df_raw
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("subtotal", F.col("subtotal").cast(DoubleType()))
        .withColumn("tax_amount", F.col("tax_amount").cast(DoubleType()))
        .withColumn("shipping_cost", F.col("shipping_cost").cast(DoubleType()))
        .withColumn("discount_amount", F.col("discount_amount").cast(DoubleType()))
        .withColumn("item_count", F.col("item_count").cast(IntegerType()))
        .withColumn("is_prime", F.col("is_prime").cast(BooleanType()))
        .withColumn("is_gift", F.col("is_gift").cast(BooleanType()))
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("month", F.col("month").cast(IntegerType()))
        .withColumn("day", F.col("day").cast(IntegerType()))
        .withColumn("hour", F.col("hour").cast(IntegerType()))
    )

    # ── Remove records with null primary keys ──
    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("user_id").isNotNull()
    )
    log_dataframe_stats(df, "AFTER_NULL_FILTER", "orders")

    # ── Null standardization for string columns ──
    df = standardize_nulls(df, ["country", "device_type", "customer_segment", "payment_method"])

    # ── Filter invalid amounts ──
    df = df.filter(
        (F.col("total_amount") > 0) &
        (F.col("total_amount") < 50000) &
        F.col("total_amount").isNotNull()
    )

    # ── Deduplicate by order_id ──
    df = deduplicate(df, ["order_id"], "event_timestamp")
    log_dataframe_stats(df, "AFTER_DEDUP", "orders")

    # ── Derived / enriched columns ──
    df = (
        df
        .withColumn("order_day_of_week", F.dayofweek("event_timestamp"))
        .withColumn("is_weekend", (F.dayofweek("event_timestamp").isin([1, 7])).cast(BooleanType()))
        .withColumn("order_hour_bucket",
            F.when(F.col("hour").between(0, 5), "late_night")
             .when(F.col("hour").between(6, 11), "morning")
             .when(F.col("hour").between(12, 17), "afternoon")
             .when(F.col("hour").between(18, 21), "evening")
             .otherwise("night")
        )
        .withColumn("order_value_tier",
            F.when(F.col("total_amount") < 50, "low")
             .when(F.col("total_amount").between(50, 200), "medium")
             .when(F.col("total_amount").between(200, 500), "high")
             .otherwise("premium")
        )
        .withColumn("effective_discount_pct",
            F.when(F.col("subtotal") > 0,
                F.round((F.col("discount_amount") / F.col("subtotal")) * 100, 2)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("data_quality_score",
            F.when(
                F.col("country").isNotNull() &
                F.col("device_type").isNotNull() &
                F.col("items").isNotNull(), "HIGH"
            ).when(
                F.col("total_amount").isNotNull(), "MEDIUM"
            ).otherwise("LOW")
        )
    )

    log_dataframe_stats(df, "SILVER_READY", "orders")
    return df


# ─────────────────────────────────────────────
# Write Silver Layer
# ─────────────────────────────────────────────

def write_silver(df: DataFrame, entity: str, partition_cols: List[str]) -> None:
    """Write transformed DataFrame to S3 Silver zone in Parquet format."""
    silver_path = get_silver_path(entity)
    logger.info(f"Writing {entity} to Silver: {silver_path}")

    (
        df.repartition(*[F.col(c) for c in partition_cols])
        .write
        .mode("append")
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 500_000)
        .parquet(silver_path)
    )
    logger.info(f"Silver write complete: {entity}")


def update_glue_catalog(entity: str, silver_path: str) -> None:
    """Update Glue Data Catalog table with new partition metadata."""
    try:
        import boto3
        glue_client = boto3.client("glue")
        glue_client.start_crawler(Name=f"ecommerce-silver-{entity}-crawler")
        logger.info(f"Triggered Glue Crawler for {entity}")
    except Exception as e:
        logger.warning(f"Could not trigger crawler for {entity}: {e}")


# ─────────────────────────────────────────────
# Main Job Execution
# ─────────────────────────────────────────────

def run():
    logger.info(f"Starting Bronze → Silver ETL Job")
    logger.info(f"Job: {JOB_NAME} | Env: {ENVIRONMENT} | Mode: {'incremental' if INCREMENTAL_MODE else 'full'}")
    logger.info(f"Processing date: {PROCESSING_DATE}")

    job_start = datetime.now(timezone.utc)
    entities_processed = []

    # ── Orders ──────────────────────────────────
    try:
        orders_bronze_path = get_bronze_path("orders", PROCESSING_DATE)
        orders_df = transform_orders(orders_bronze_path)
        if orders_df.count() > 0:
            write_silver(orders_df, "orders", ["year", "month", "day"])
            update_glue_catalog("orders", get_silver_path("orders"))
            entities_processed.append("orders")
    except Exception as e:
        logger.error(f"Orders transformation failed: {e}", exc_info=True)
        raise

    elapsed = (datetime.now(timezone.utc) - job_start).total_seconds()
    logger.info(f"Job complete | elapsed={elapsed:.1f}s | entities={entities_processed}")


run()
job.commit()
