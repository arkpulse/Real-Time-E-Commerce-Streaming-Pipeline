# 🛒 Real-Time E-Commerce Streaming Data Pipeline

[![CI/CD](https://github.com/your-org/ecommerce-streaming-pipeline/actions/workflows/ci_cd.yml/badge.svg)](https://github.com/your-org/ecommerce-streaming-pipeline/actions)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Lambda%20%7C%20Glue%20%7C%20S3-orange.svg)](https://aws.amazon.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8.svg)](https://snowflake.com/)
[![Terraform](https://img.shields.io/badge/Terraform-1.6-purple.svg)](https://terraform.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docker.com/)

A **production-grade, cloud-native streaming data engineering system** that simulates a real e-commerce platform generating millions of events per day. Built to demonstrate end-to-end data engineering proficiency across ingestion, processing, storage, and visualization.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA PRODUCER (Docker)                       │
│   Python + Faker → Realistic E-Commerce Events (50+ events/sec)     │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  JSON Payloads
          ┌────────────────▼────────────────────┐
          │         AWS KINESIS DATA STREAMS      │
          │  orders │ payments │ clicks │ cart    │
          └────────┬──────────┬──────────┬───────┘
                   │          │          │
        ┌──────────▼──┐  ┌────▼────┐  ┌─▼───────┐
        │   Lambda    │  │ Lambda  │  │ Lambda  │
        │  (Orders)   │  │ (Pay.)  │  │ (Click) │
        └──────┬──────┘  └────┬────┘  └────┬────┘
               │ Validate      │ Fraud       │ Schema
               │ Dedupe        │ Check       │ Enrich
               │ Enrich        │             │
               └──────────┬────┘─────────────┘
                           │
          ┌────────────────▼────────────────────┐
          │           AMAZON S3 (Data Lake)      │
          │  ┌─────────┐ ┌────────┐ ┌────────┐  │
          │  │ BRONZE  │ │SILVER  │ │  GOLD  │  │
          │  │  Raw    │ │Cleaned │ │  KPIs  │  │
          │  │  JSON   │ │Parquet │ │Parquet │  │
          │  └────┬────┘ └───┬────┘ └────┬───┘  │
          └───────┼──────────┼───────────┼───────┘
                  │          │           │
          ┌───────▼──────────▼───────────▼──────┐
          │           AWS GLUE (PySpark ETL)     │
          │  Bronze→Silver  │  Silver→Gold       │
          │  Dedupe/Clean   │  KPIs/Aggs         │
          └────────────────┬─────────────────────┘
                           │
               ┌───────────┴───────────┐
               │                       │
    ┌──────────▼──────────┐  ┌────────▼────────┐
    │       ATHENA         │  │    SNOWFLAKE     │
    │  Ad-hoc S3 queries   │  │  Star Schema DW  │
    │  Conversion/Funnel   │  │  Fact+Dim Tables │
    └──────────────────────┘  └────────┬─────────┘
                                        │
                             ┌──────────▼──────────┐
                             │       POWER BI        │
                             │  6 Dashboard Pages    │
                             │  Real-Time Metrics    │
                             └──────────────────────┘
```

---

## 📁 Project Structure

```
ecommerce-streaming-pipeline/
│
├── producer/                       # Python event generator service
│   ├── event_generator.py          # Core fake event factory (all entity types)
│   ├── kinesis_producer.py         # Batching, retry, metrics Kinesis client
│   ├── main.py                     # Orchestrator — multi-threaded streaming
│   ├── requirements.txt
│   └── Dockerfile                  # Multi-stage production image
│
├── lambda/
│   ├── orders_processor/           # Orders Kinesis consumer
│   │   └── handler.py              # Schema validation, dedup, DQ, S3 write
│   ├── payments_processor/         # Payments + fraud signal detection
│   │   └── handler.py
│   ├── clickstream_processor/      # High-throughput page-view handler
│   │   └── handler.py
│   └── cart_processor/             # Cart lifecycle + abandonment alerts
│       └── handler.py
│
├── glue/
│   ├── bronze_to_silver/
│   │   └── orders_transformation.py    # PySpark Bronze→Silver ETL
│   └── silver_to_gold/
│       └── gold_aggregations.py        # Fact tables, dims, KPI aggregates
│
├── snowflake/
│   ├── ddl/
│   │   └── 01_create_tables.sql        # Full star schema DDL
│   └── dml/
│       ├── 02_merge_procedures.sql     # MERGE upsert stored procedures
│       └── 03_snowpipe_and_tasks.sql   # Snowpipe + scheduled tasks
│
├── athena/
│   └── queries/
│       └── analytics_queries.sql       # 10 production analytical queries
│
├── data_quality/
│   └── dq_framework.py                 # Reusable DQ engine (checks + reports)
│
├── terraform/
│   └── main.tf                         # Full AWS infra-as-code (S3, Kinesis,
│                                       # Lambda, Glue, DynamoDB, SNS, CW)
│
├── tests/
│   └── unit/
│       └── test_pipeline.py            # 30+ pytest tests with moto mocking
│
├── scripts/
│   └── localstack-init.sh              # Local dev AWS resource provisioning
│
├── monitoring/
│   └── prometheus.yml
│
├── docs/
│   └── POWERBI_GUIDE.md               # DAX measures + dashboard specs
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml                  # Full CI/CD: lint→test→build→deploy
│
├── docker-compose.yml                 # Producer + monitor + LocalStack + Grafana
├── .env.example                       # All config variables documented
└── README.md
```

---

## ⚡ Quick Start

### Prerequisites

| Tool       | Version  |
|------------|----------|
| Python     | 3.11+    |
| Docker     | 24+      |
| AWS CLI    | 2.x      |
| Terraform  | 1.6+     |
| Snowflake  | account  |

### 1. Clone & Configure

```bash
git clone https://github.com/your-org/ecommerce-streaming-pipeline.git
cd ecommerce-streaming-pipeline
cp .env.example .env
# Edit .env with your AWS credentials and resource names
```

### 2. Local Development (LocalStack)

```bash
# Start all local services including LocalStack AWS emulation
docker compose --profile local-dev up -d

# Check LocalStack is healthy
curl http://localhost:4566/_localstack/health

# Start producer streaming to LocalStack
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
docker compose up producer

# Tail producer logs
docker compose logs -f producer
```

### 3. Deploy to AWS

```bash
# Authenticate with AWS
aws configure

# Provision all infrastructure
cd terraform
terraform init
terraform plan -var="alert_email=you@company.com"
terraform apply

# Upload Glue scripts to S3
aws s3 cp glue/ s3://your-bucket/scripts/ --recursive

# Package and deploy Lambda functions
./scripts/deploy_lambdas.sh prod

# Start the producer (ECS Fargate or local Docker)
docker compose up -d producer
```

### 4. Run Tests

```bash
pip install pytest pytest-cov moto[kinesis,s3,dynamodb,sns] boto3 faker
pytest tests/unit/ -v --cov=producer --cov=data_quality --cov-report=term-missing
```

---

## 🔧 Tech Stack Details

| Layer             | Technology                              | Purpose                              |
|-------------------|-----------------------------------------|--------------------------------------|
| Event Generation  | Python 3.11, Faker                      | Realistic synthetic streaming events |
| Streaming         | AWS Kinesis Data Streams (4 streams)    | High-throughput event ingestion      |
| Stream Processing | AWS Lambda (Python 3.11)                | Serverless consumers per event type  |
| Raw Storage       | Amazon S3 (Parquet + NDJSON)            | Immutable Bronze lake zone           |
| ETL Processing    | AWS Glue 4.0 + PySpark 3.3              | Medallion architecture transforms    |
| Query Engine      | Amazon Athena                           | SQL analytics on S3 Parquet          |
| Data Warehouse    | Snowflake                               | Star schema, fact + dimension tables |
| Orchestration     | Snowflake Tasks + AWS Glue Triggers     | Incremental pipeline scheduling      |
| Visualization     | Power BI                                | 6-page executive dashboard           |
| IaC               | Terraform 1.6                           | Full AWS resource provisioning       |
| Containerization  | Docker + Docker Compose                 | Local dev + producer deployment      |
| CI/CD             | GitHub Actions                          | Lint → test → build → deploy         |
| Monitoring        | CloudWatch + SNS + Prometheus/Grafana   | Alerts, metrics, dashboards          |
| Data Quality      | Custom Python DQ framework              | Schema, null, dedup, anomaly checks  |
| Deduplication     | DynamoDB (TTL-based)                    | Exactly-once order/payment processing|

---

## 📊 Event Types Generated

| Stream              | Events/Sec | Description                                        |
|---------------------|------------|----------------------------------------------------|
| `orders_stream`     | ~7.5       | Full order with items, pricing, shipping, coupons  |
| `payments_stream`   | ~5         | Payment transactions with gateway & fraud signals  |
| `clickstream_stream`| ~25        | Page views, search queries, time-on-page, scroll   |
| `cart_stream`       | ~12.5      | Add/remove/abandon/save cart events                |

---

## 🗄️ Snowflake Star Schema

```
                    ┌──────────────┐
                    │  DIM_TIME    │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
┌─────────────┐    ┌───────▼────────┐    ┌──────────────┐
│  DIM_USERS  │◄───│  FACT_ORDERS   │───►│ DIM_PRODUCTS │
│  (user_sk)  │    │                │    │ (product_sk) │
└─────────────┘    │ order_id       │    └──────────────┘
                   │ user_sk   FK   │
┌─────────────┐    │ device_sk FK   │    ┌──────────────┐
│ DIM_DEVICES │◄───│ date_key  FK   │    │FACT_PAYMENTS │
│ (device_sk) │    │ order_total    │    │ (payment_id) │
└─────────────┘    │ item_count     │    └──────────────┘
                   │ ...            │
                   └────────────────┘    ┌──────────────────┐
                                         │ FACT_CLICKSTREAM  │
                                         │ (event_id)        │
                                         └──────────────────┘
```

---

## 🏅 Medallion Architecture

| Layer      | Format          | Location                       | Description                              |
|------------|-----------------|--------------------------------|------------------------------------------|
| **Bronze** | NDJSON          | `s3://bucket/bronze/`          | Raw, immutable events as ingested        |
| **Silver** | Parquet (Snappy)| `s3://bucket/silver/`          | Cleaned, typed, deduped, enriched        |
| **Gold**   | Parquet (Snappy)| `s3://bucket/gold/`            | Fact/dim tables and KPI aggregates       |
| **DLQ**    | JSON            | `s3://bucket/dlq/`             | Failed/invalid records for investigation |

All layers partitioned by `year/month/day/[hour]`.

---

## 📈 Data Quality Checks

The DQ framework runs **per-record, per-batch** checks across three categories:

| Category      | Checks                                                        |
|---------------|---------------------------------------------------------------|
| Completeness  | Required field not-null checks for all primary keys          |
| Validity      | Amount ranges, enum validation, timestamp format, ID prefixes|
| Timeliness    | No future timestamps, ingestion lag monitoring               |

Quality scores are published to **CloudWatch** and trigger **SNS alerts** if the batch failure rate exceeds 10%.

---

## 🚨 Alerting (AWS SNS)

| Alert                         | Trigger                                      |
|-------------------------------|----------------------------------------------|
| Lambda Error Rate             | > 10 errors in 5-minute window               |
| Kinesis Iterator Age          | Consumer lag > 5 minutes                     |
| DQ Failure Rate               | Batch failure rate > 10%                     |
| S3 Write Failure              | Any Lambda batch fails to persist to S3      |
| Glue Job Failure              | Bronze→Silver or Silver→Gold job fails       |
| Cart Abandonment              | Real-time SNS notifications for remarketing  |

---

## 🔄 CI/CD Pipeline (GitHub Actions)

```
Push → Lint (black/flake8/isort)
     → Unit Tests (pytest + moto)
     → Security Scan (bandit + gitleaks)
     → Terraform Plan (PR only)
     → Docker Build & Push (ECR)
     → Package Lambdas (zip → S3)
     → Deploy Staging (develop branch)
     → Deploy Production (main branch)
     → Slack Notification
```

---

## 💼 Resume Bullet Points

> Copy-paste ready for your CV:

- **Designed and built** an end-to-end real-time e-commerce streaming pipeline processing 50+ events/second across 4 Kinesis streams using Python, AWS Lambda, and PySpark on AWS Glue
- **Implemented Medallion Architecture** (Bronze/Silver/Gold) on Amazon S3 with Parquet storage, partition pruning, and schema evolution using AWS Glue 4.0 and PySpark 3.3
- **Engineered Snowflake star schema** with 3 fact tables and 4 dimension tables, Snowpipe auto-ingest, MERGE upsert procedures, and incremental ETL via scheduled Snowflake Tasks
- **Built serverless DQ framework** with 15+ configurable checks per entity (schema, null, dedup, anomaly), publishing quality scores to CloudWatch with SNS alerting
- **Deployed full infrastructure-as-code** using Terraform for Kinesis, S3, Lambda, Glue, DynamoDB, SNS, and IAM across dev/staging/prod environments
- **Containerized and orchestrated** the producer service with Docker Compose, achieving configurable throughput of 10–500 events/second via multi-threaded workers
- **Built CI/CD pipeline** with GitHub Actions covering lint, pytest (30+ tests with moto mocking), security scanning, Docker build, Lambda packaging, and blue/green deployment

---

## 🎤 Interview Q&A

**Q: Why Kinesis over Kafka for this project?**
> Kinesis is fully managed with no cluster administration overhead, native Lambda integration, and transparent scaling. For a team without dedicated infrastructure engineers, the operational simplicity outweighs Kafka's higher throughput ceiling. For >100K events/sec, MSK would be appropriate.

**Q: How do you handle late-arriving data in the Silver layer?**
> The Glue ETL uses window functions (`ROW_NUMBER OVER PARTITION BY order_id ORDER BY event_timestamp DESC`) to pick the most recent record per key. Glue job bookmarks ensure only new partitions are processed incrementally, but we re-process the last 3 hours to catch late arrivals within SLA.

**Q: Why DynamoDB for deduplication instead of Kinesis Enhanced Fan-Out?**
> DynamoDB provides cross-Lambda-invocation dedup with TTL-based expiration (24h). Enhanced Fan-Out solves fan-out parallelism, not idempotency. For exactly-once semantics across Lambda retries and Kinesis reprocessing, a persistent store is required.

**Q: How does the Medallion Architecture improve query performance?**
> Bronze stores raw NDJSON for auditability. Silver applies Parquet+Snappy compression (typically 5-10× size reduction) with column pruning and partition elimination for Athena. Gold pre-aggregates business KPIs, reducing Snowflake query compute from full table scans to pre-computed summaries.

**Q: How would you scale this to 10× the current throughput?**
> (1) Increase Kinesis shard count (each shard handles 1MB/s ingestion). (2) Raise Lambda parallelization_factor to 10 per shard. (3) Switch Glue to G.2X workers. (4) Snowflake: scale warehouse to LARGE during load windows. (5) Add Kinesis Data Analytics for sub-second windowed aggregations without Lambda.

**Q: What is your data recovery strategy if a Glue job fails mid-run?**
> Glue job bookmarks record the last successfully processed S3 prefix. On rerun, processing resumes from the checkpoint. Bronze data is immutable, so no source data is lost. The Silver/Gold writes use `mode=append` with partition paths, so partial writes are either completed or the partition is dropped and reprocessed.

---

## 🌐 LinkedIn Project Description

**Real-Time E-Commerce Streaming Data Pipeline | AWS · Snowflake · PySpark · Python**

Built a production-grade end-to-end streaming data engineering system simulating a live e-commerce platform at 50+ events/second. The architecture spans 4 AWS Kinesis streams consumed by serverless Lambda functions that validate, deduplicate, and enrich events before landing them in a partitioned S3 data lake. AWS Glue PySpark jobs implement a full Medallion Architecture (Bronze → Silver → Gold), feeding a Snowflake star schema data warehouse with continuous Snowpipe auto-ingest and MERGE-based upserts. Analytical workloads run on both Athena (ad-hoc SQL on S3 Parquet) and Snowflake, with results visualized in a 6-page Power BI executive dashboard. Infrastructure is fully managed with Terraform, the producer is containerized with Docker, and the project ships with a GitHub Actions CI/CD pipeline covering lint, 30+ unit tests with moto AWS mocking, security scanning, and blue/green deployment.

**Tech:** Python 3.11 · AWS Kinesis · AWS Lambda · Amazon S3 · AWS Glue · PySpark · Parquet · Snowflake · Athena · Power BI · Terraform · Docker · GitHub Actions

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
