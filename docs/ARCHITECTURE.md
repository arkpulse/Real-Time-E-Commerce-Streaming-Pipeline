# Architecture Deep-Dive

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PRODUCER LAYER  (Docker Container — Python 3.11)                          │
│                                                                             │
│   EventGenerator                 KinesisProducer                           │
│   ┌────────────────────┐         ┌───────────────────────────────────────┐ │
│   │  generate_order()  │──JSON──▶│  buffer_event()  batch_size=100       │ │
│   │  generate_payment()│         │  flush_stream()  retry=3, backoff     │ │
│   │  generate_click()  │         │  metrics tracker (rps, bytes, errors) │ │
│   │  generate_cart()   │         └────────────┬──────────────────────────┘ │
│   └────────────────────┘                      │  put_records API            │
│   50 events/sec • 4 workers                   │  partition_key = user_id    │
└───────────────────────────────────────────────┼─────────────────────────────┘
                                                │
                    ┌───────────────────────────▼─────────────────────────────┐
                    │   AWS KINESIS DATA STREAMS                              │
                    │                                                         │
                    │  ┌────────────┐ ┌────────────┐ ┌──────────┐ ┌───────┐ │
                    │  │orders      │ │payments    │ │clickstrm │ │cart   │ │
                    │  │4 shards    │ │4 shards    │ │4 shards  │ │4 shard│ │
                    │  │KMS encrypt │ │KMS encrypt │ │KMS enc.  │ │KMS enc│ │
                    │  └─────┬──────┘ └─────┬──────┘ └────┬─────┘ └──┬────┘ │
                    └────────┼──────────────┼──────────────┼───────────┼──────┘
                             │              │              │           │
              ┌──────────────▼──┐   ┌───────▼───┐  ┌──────▼──┐  ┌───▼─────┐
              │ Lambda          │   │ Lambda    │  │ Lambda  │  │ Lambda  │
              │ orders_proc     │   │ payments  │  │ clicks  │  │ cart    │
              │ 512MB/300s      │   │ 512MB     │  │ 256MB   │  │ 512MB   │
              │ batch=100       │   │ batch=100 │  │ batch   │  │ batch   │
              │ para_factor=4   │   │ fraud det.│  │ =100    │  │ abandon │
              │                 │   │           │  │         │  │ alerts  │
              │ 1.validate_schema│  └─────┬─────┘  └────┬────┘  └────┬────┘
              │ 2.business_rules│        │              │            │
              │ 3.dedup(DynDB)  │        │              │            │
              │ 4.enrich        │        │              │            │
              │ 5.write_bronze  │        │              │            │
              └────────┬────────┘        │              │            │
                       │                 │              │            │
              ┌────────▼─────────────────▼──────────────▼────────────▼───────┐
              │  AMAZON S3 — Data Lake (ecommerce-data-lake)                 │
              │                                                               │
              │  bronze/orders/year=*/month=*/day=*/hour=*/     ← NDJSON     │
              │  bronze/payments/year=*/month=*/day=*/hour=*/                │
              │  bronze/clickstream/year=*/month=*/day=*/hour=*/             │
              │  bronze/carts/year=*/month=*/day=*/hour=*/                   │
              │  dlq/orders/   dlq/payments/   dlq/clickstream/  ← failed   │
              └────────────────────────────────────────────────────────────┬─┘
                                                                           │
              ┌────────────────────────────────────────────────────────────▼─┐
              │  AWS GLUE 4.0 — PySpark ETL (Medallion Architecture)        │
              │                                                               │
              │  Job 1: bronze_to_silver (G.1X × 5 workers, hourly)         │
              │    ▸ read NDJSON → cast types → null handling                │
              │    ▸ dedup (window ROW_NUMBER) → enrich → write Parquet     │
              │                                                               │
              │  Job 2: silver_to_gold (G.1X × 5 workers, daily)            │
              │    ▸ build fact_orders, fact_payments, fact_clickstream      │
              │    ▸ build dim_users, dim_products, dim_time, dim_devices    │
              │    ▸ build agg_daily_revenue, agg_product_performance,      │
              │         agg_funnel, agg_hourly_revenue                       │
              │                                                               │
              │  silver/orders/year=*/month=*/day=*/    ← Parquet+Snappy    │
              │  silver/payments/  silver/clickstream/  silver/carts/        │
              │  gold/fact_orders/  gold/fact_payments/  gold/fact_clicks/   │
              │  gold/dim_*/  gold/agg_*/                                    │
              └────────────────────────┬──────────────────────────────────┬─┘
                                       │                                  │
              ┌────────────────────────▼──────┐          ┌───────────────▼───┐
              │  AMAZON ATHENA                │          │  SNOWFLAKE DW     │
              │                              │          │                   │
              │  ecommerce_catalog database  │          │  ECOMMERCE_DW.    │
              │  Glue Crawlers → tables      │          │  ANALYTICS schema │
              │  Parquet partition pruning   │          │                   │
              │                              │          │  ┌─────────────┐  │
              │  10 analytical SQL queries:  │          │  │ Snowpipe    │  │
              │  • top products              │          │  │ AUTO_INGEST │  │
              │  • conversion rates          │          │  │ Gold→stg    │  │
              │  • hourly revenue            │          │  └──────┬──────┘  │
              │  • cart abandonment          │          │         │MERGE    │
              │  • cohort retention          │          │  FACT_ORDERS      │
              │  • active users              │          │  FACT_PAYMENTS    │
              │  • payment success rates     │          │  FACT_CLICKSTREAM │
              │  • search performance        │          │  DIM_USERS        │
              │  • DQ monitoring             │          │  DIM_PRODUCTS     │
              └──────────────────────────────┘          │  DIM_TIME         │
                                                        │  DIM_DEVICES      │
                                                        │  + 6 Views        │
                                                        │  + Tasks/15min    │
                                                        └──────────┬────────┘
                                                                   │
                                                     ┌─────────────▼──────────┐
                                                     │  POWER BI              │
                                                     │                        │
                                                     │  Page 1: Executive     │
                                                     │  Page 2: Sales         │
                                                     │  Page 3: Real-Time     │
                                                     │  Page 4: User Behavior │
                                                     │  Page 5: Products      │
                                                     │  Page 6: Funnel        │
                                                     └────────────────────────┘

SUPPORTING SERVICES
───────────────────
DynamoDB (dedup):  ecommerce-dedup-orders, ecommerce-dedup-payments (TTL 24h)
SNS Alerts:        pipeline failures, DQ alerts, cart abandonment, fraud flags
CloudWatch:        Lambda errors/duration/throttles, Kinesis iterator age,
                   Glue failures, DQ pass rate — dashboard + 12 alarms
KMS:               Kinesis stream encryption
IAM:               Least-privilege roles for Lambda + Glue
Terraform:         All resources as code, 3 environments (dev/staging/prod)
GitHub Actions:    lint → test → security → docker build → lambda pkg → deploy
Docker Compose:    producer + LocalStack + Prometheus + Grafana
```

## Component Responsibilities

| Component | Responsibility | Key Design Decision |
|-----------|---------------|---------------------|
| EventGenerator | Synthetic e-commerce events with session coherence | Maintains session pool (up to 200) for realistic user journeys |
| KinesisProducer | Buffered, batched, retried delivery to Kinesis | Exponential backoff; partition key = user_id for ordered per-user delivery |
| Lambda (Orders) | Schema validate, dedup, enrich, write bronze | DynamoDB for cross-invocation dedup; fail-open on DDB errors |
| Lambda (Payments) | As orders + fraud signal detection | Non-blocking fraud signals — flagged not rejected |
| Lambda (Clickstream) | High-throughput, lightweight validation | 256 MB memory; no dedup (event_id is globally unique) |
| Lambda (Cart) | Cart lifecycle + SNS abandonment notifications | Real-time abandonment published for remarketing |
| Glue Bronze→Silver | Type casting, null handling, dedup, enrichment | Job bookmarks for incremental; window dedup for late arrivals |
| Glue Silver→Gold | Fact/dim tables, KPI aggregates | Columnar Parquet with partition pruning for Athena and Snowflake COPY |
| Snowflake | Star schema DW with incremental MERGE upserts | Snowpipe for continuous ingest; 15-min Task for merge + reporting |
| Athena | Ad-hoc analytics on S3 Parquet | Partition elimination saves up to 95% of data scanned |
| DQ Framework | Per-record, per-batch checks with CloudWatch metrics | 15+ configurable checks; alerts at >10% failure rate |
| Terraform | Full IaC for all AWS resources | 3 env tfvars; backend in S3 + DynamoDB state lock |
