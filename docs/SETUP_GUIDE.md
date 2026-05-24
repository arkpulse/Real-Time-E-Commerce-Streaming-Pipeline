# Setup & Deployment Guide

## Prerequisites

| Tool       | Version | Install                                   |
|------------|---------|-------------------------------------------|
| Python     | 3.11+   | https://python.org                        |
| Docker     | 24+     | https://docker.com                        |
| AWS CLI    | 2.x     | `pip install awscli`                      |
| Terraform  | 1.6+    | https://terraform.io/downloads            |
| Git        | 2.x     | https://git-scm.com                       |
| Snowflake  | account | https://signup.snowflake.com              |

---

## 1. Local Development Setup (15 minutes)

### Clone and configure

```bash
git clone https://github.com/your-org/ecommerce-streaming-pipeline.git
cd ecommerce-streaming-pipeline

# Copy and edit environment config
cp .env.example .env
# Edit .env — for local dev, you only need:
#   AWS_REGION=us-east-1
#   AWS_ACCESS_KEY_ID=test
#   AWS_SECRET_ACCESS_KEY=test
```

### Install dev dependencies

```bash
pip install -r requirements-dev.txt
# or
make install
```

### Run unit tests

```bash
make test-unit
# Expected: 30+ tests passing, ~70% coverage
```

### Start local AWS environment (LocalStack)

```bash
# Start LocalStack + producer + monitoring
make docker-up-dev

# Verify LocalStack is healthy
curl -s http://localhost:4566/_localstack/health | python -m json.tool

# Check Kinesis streams were created
aws --endpoint-url=http://localhost:4566 kinesis list-streams

# Tail producer logs
make docker-logs
```

### Run integration tests against LocalStack

```bash
make test-integration
```

---

## 2. AWS Cloud Deployment

### Step 1: Configure AWS credentials

```bash
aws configure
# AWS Access Key ID: <your key>
# AWS Secret Access Key: <your secret>
# Default region: us-east-1
# Default output format: json

# Verify
aws sts get-caller-identity
```

### Step 2: Create Terraform state backend

```bash
# Create S3 bucket for Terraform state
aws s3 mb s3://ecommerce-terraform-state --region us-east-1
aws s3api put-bucket-versioning \
    --bucket ecommerce-terraform-state \
    --versioning-configuration Status=Enabled

# Create DynamoDB table for state locking
aws dynamodb create-table \
    --table-name terraform-state-lock \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1
```

### Step 3: Deploy infrastructure

```bash
cd terraform

# Initialise
terraform init

# Review the plan
ALERT_EMAIL=you@company.com make tf-plan ENVIRONMENT=prod

# Apply (creates ~25 AWS resources)
make tf-apply ENVIRONMENT=prod

# Copy the output .env snippet
terraform output env_snippet
```

### Step 4: Upload Glue scripts

```bash
# Get bucket name from Terraform output
BUCKET=$(terraform output -raw s3_bucket_name)

aws s3 sync glue/ s3://$BUCKET/scripts/ \
    --exclude "*.pyc" \
    --exclude "__pycache__/*"
```

### Step 5: Deploy Lambda functions

```bash
ENVIRONMENT=prod make deploy-lambdas
# Packages each function, uploads to S3, updates Lambda code
# For prod: also publishes version and updates LIVE alias
```

### Step 6: Start the producer

```bash
# Option A: local Docker → production Kinesis
docker compose up -d producer

# Option B: deploy to ECS Fargate (recommended for prod)
# See docs/ECS_DEPLOYMENT.md
```

---

## 3. Snowflake Setup

### Step 1: Connect to Snowflake

```bash
# Using SnowSQL CLI
snowsql -a <account> -u <user>
```

### Step 2: Run DDL scripts in order

```sql
-- In Snowflake worksheet or SnowSQL:
!source snowflake/ddl/01_create_tables.sql
!source snowflake/dml/02_merge_procedures.sql
!source snowflake/dml/03_snowpipe_and_tasks.sql
!source snowflake/ddl/04_analytics_views.sql
```

### Step 3: Configure S3 Storage Integration

```sql
-- After running DDL:
DESC INTEGRATION S3_ECOMMERCE_INTEGRATION;
-- Copy the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Then update your S3 bucket trust policy with these values
```

### Step 4: Enable Snowpipe

```sql
-- Get the SQS ARN for S3 event notifications
SELECT SYSTEM$PIPE_STATUS('ECOMMERCE_DW.RAW.PIPE_FACT_ORDERS');

-- Configure S3 bucket notification to point to the pipe's SQS queue
-- (use the notification_channel from the pipe status output)
```

### Step 5: Start scheduled task

```sql
USE DATABASE ECOMMERCE_DW;
USE SCHEMA ANALYTICS;
ALTER TASK FULL_LOAD_TASK RESUME;

-- Verify it's scheduled
SHOW TASKS;
```

---

## 4. Power BI Setup

1. Open **Power BI Desktop**
2. **Get Data** → **Snowflake**
3. Server: `<account>.snowflakecomputing.com`
4. Warehouse: `REPORTING_WH`
5. Import tables listed in `docs/POWERBI_GUIDE.md`
6. Set up relationships as described in the guide
7. Paste DAX measures from `docs/POWERBI_GUIDE.md`
8. Build the 6 pages per the specifications
9. Publish to Power BI Service
10. Configure scheduled refresh (15-minute interval)

---

## 5. Running the Glue Pipeline Manually

```bash
# Run pipeline for today
make glue-run ENVIRONMENT=prod

# Run pipeline for a specific date (backfill)
make glue-run ENVIRONMENT=prod PROCESSING_DATE=2025-05-01

# Monitor in AWS console:
# Glue → Jobs → ecommerce-bronze-to-silver-prod → Run history
```

---

## 6. Monitoring & Alerts

### CloudWatch Dashboard

```
AWS Console → CloudWatch → Dashboards → ecommerce-pipeline-prod
```

Dashboard shows:
- Lambda invocations, errors, duration p95 (all 4 functions)
- Kinesis iterator age and write throughput (all streams)
- DQ pass rate % and critical failures

### SNS Alerts

Edit `.env`:
```
ALERT_EMAIL=you@company.com
```

You will receive email alerts for:
- Lambda error rate > threshold
- Kinesis consumer falling behind > 5 minutes
- DQ failure rate > 10%
- Glue job failures
- S3 write failures

### Grafana (local dev)

```bash
make docker-up-dev
open http://localhost:3000  # admin / admin123
```

---

## 7. Testing the Full Pipeline

```bash
# 1. Start producer locally against real AWS
docker compose up -d producer

# 2. Watch events flowing into Kinesis
aws kinesis get-shard-iterator \
    --stream-name ecommerce-orders-stream \
    --shard-id shardId-000000000000 \
    --shard-iterator-type LATEST \
    --query ShardIterator --output text | xargs -I {} \
aws kinesis get-records --shard-iterator {} --limit 5

# 3. Check S3 Bronze landing (after ~30 seconds)
aws s3 ls s3://$BUCKET/bronze/orders/ --recursive | head -10

# 4. Run Glue Bronze→Silver
make glue-run ENVIRONMENT=prod

# 5. Check Silver Parquet
aws s3 ls s3://$BUCKET/silver/orders/ --recursive | head -10

# 6. Query via Athena
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) FROM ecommerce_catalog.silver_orders" \
    --query-execution-context Database=ecommerce_catalog \
    --result-configuration OutputLocation=s3://$BUCKET/athena-results/

# 7. Check Snowflake after Snowpipe ingest
# SELECT COUNT(*) FROM ECOMMERCE_DW.ANALYTICS.FACT_ORDERS;
```

---

## 8. Cost Estimates (AWS — us-east-1)

| Service          | Dev/Month | Prod/Month (50 events/sec) |
|------------------|-----------|---------------------------|
| Kinesis (4+1 streams × 4 shards) | ~$18 | ~$72 |
| Lambda (4 functions, ~4M invocations) | ~$2 | ~$15 |
| S3 (Bronze+Silver+Gold, ~50 GB/mo) | ~$2 | ~$8 |
| Glue (5 workers × 2 jobs, daily) | ~$5 | ~$25 |
| DynamoDB (dedup, on-demand) | ~$1 | ~$5 |
| CloudWatch (metrics + logs) | ~$3 | ~$15 |
| **Total AWS** | **~$31** | **~$140** |
| Snowflake (XSMALL/SMALL warehouse) | ~$50 | ~$200 |
| **Total** | **~$81** | **~$340** |

*Estimates only. Actual costs depend on usage patterns.*

---

## 9. Troubleshooting

| Issue | Check |
|-------|-------|
| Producer not sending | Check `.env` AWS credentials; run `aws kinesis list-streams` |
| Lambda errors in CloudWatch | Check `Function logs` → common: DynamoDB timeout, S3 permissions |
| Kinesis iterator age growing | Scale shards or Lambda parallelization_factor |
| Glue job failing | Check Glue job run logs in CloudWatch; verify S3 path exists |
| Snowpipe not ingesting | `SELECT SYSTEM$PIPE_STATUS(...)` — check SQS queue notification |
| DQ alerts firing | Check `dlq/` prefix in S3 for failed records and failure reasons |
