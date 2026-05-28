# =============================================================
# Terraform: E-Commerce Streaming Pipeline Infrastructure
# =============================================================
# Provisions all AWS resources: Kinesis, S3, Lambda, Glue,
# SNS, DynamoDB, IAM roles, and CloudWatch alarms.
#
# Author:  Data Engineering Team
# Version: 1.0.0
# =============================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "ecommerce-terraform-state"
    key            = "streaming-pipeline/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "ecommerce-streaming-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
      Team        = "data-engineering"
    }
  }
}

# ─────────────────────────────────────────────
# Variables
# ─────────────────────────────────────────────

variable "aws_region"   { default = "us-east-1" }
variable "environment"  { default = "prod" }
variable "project_name" { default = "ecommerce-streaming" }

variable "kinesis_shard_count" {
  description = "Number of shards per Kinesis stream"
  default     = 4
}

variable "lambda_memory_mb" {
  description = "Lambda function memory in MB"
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds"
  default     = 300
}

variable "s3_bucket_name" {
  description = "S3 data lake bucket name"
  default     = "ecommerce-data-lake"
}

variable "glue_worker_type"  { default = "G.1X" }
variable "glue_worker_count" { default = 5 }

variable "alert_email" {
  description = "Email address for pipeline alerts"
  type        = string
}


# ─────────────────────────────────────────────
# S3 Data Lake
# ─────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.s3_bucket_name}-${var.environment}"
  force_destroy = false
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-retention"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }

  rule {
    id     = "silver-retention"
    status = "Enabled"
    filter { prefix = "silver/" }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    expiration { days = 730 }
  }

  rule {
    id     = "gold-retention"
    status = "Enabled"
    filter { prefix = "gold/" }
    expiration { days = 1095 }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# ─────────────────────────────────────────────
# Kinesis Data Streams
# ─────────────────────────────────────────────

locals {
  streams = {
    orders      = "ecommerce-orders-stream-${var.environment}"
    payments    = "ecommerce-payments-stream-${var.environment}"
    clickstream = "ecommerce-clickstream-stream-${var.environment}"
    cart        = "ecommerce-cart-stream-${var.environment}"
    dlq         = "ecommerce-dlq-stream-${var.environment}"
  }
}

resource "aws_kinesis_stream" "streams" {
  for_each        = local.streams
  name            = each.value
  shard_count     = each.key == "dlq" ? 1 : var.kinesis_shard_count
  retention_period = 24  # hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis.arn

  tags = { Stream = each.key }
}

resource "aws_kms_key" "kinesis" {
  description             = "KMS key for Kinesis stream encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}


# ─────────────────────────────────────────────
# DynamoDB (Dedup Tables)
# ─────────────────────────────────────────────

resource "aws_dynamodb_table" "dedup_orders" {
  name           = "ecommerce-dedup-orders-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "order_id"

  attribute {
    name = "order_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_dynamodb_table" "dedup_payments" {
  name           = "ecommerce-dedup-payments-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "payment_id"

  attribute {
    name = "payment_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}


# ─────────────────────────────────────────────
# IAM Role: Lambda
# ─────────────────────────────────────────────

resource "aws_iam_role" "lambda_exec" {
  name = "ecommerce-lambda-exec-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "ecommerce-lambda-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListStreams"]
        Resource = [for s in aws_kinesis_stream.streams : s.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
        Resource = ["${aws_s3_bucket.data_lake.arn}", "${aws_s3_bucket.data_lake.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem"]
        Resource = [aws_dynamodb_table.dedup_orders.arn, aws_dynamodb_table.dedup_payments.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.pipeline_alerts.arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = aws_kms_key.kinesis.arn
      }
    ]
  })
}


# ─────────────────────────────────────────────
# Lambda Functions
# ─────────────────────────────────────────────

locals {
  lambda_env = {
    S3_BUCKET       = aws_s3_bucket.data_lake.bucket
    SNS_ALERT_ARN   = aws_sns_topic.pipeline_alerts.arn
    ENVIRONMENT     = var.environment
    LOG_LEVEL       = "INFO"
    DEDUP_TABLE     = aws_dynamodb_table.dedup_orders.name
  }
}

resource "aws_lambda_function" "orders_processor" {
  function_name = "ecommerce-orders-processor-${var.environment}"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"
  memory_size   = var.lambda_memory_mb
  timeout       = var.lambda_timeout_seconds
  filename      = "../lambda/orders_processor/deployment.zip"

  environment { variables = local.lambda_env }

  dead_letter_config {
    target_arn = aws_sns_topic.pipeline_alerts.arn
  }

  tracing_config { mode = "Active" }
}

resource "aws_lambda_event_source_mapping" "orders_kinesis" {
  event_source_arn              = aws_kinesis_stream.streams["orders"].arn
  function_name                 = aws_lambda_function.orders_processor.arn
  starting_position             = "LATEST"
  batch_size                    = 100
  parallelization_factor        = 4
  bisect_batch_on_function_error = true
  maximum_retry_attempts        = 3

  destination_config {
    on_failure {
      destination_arn = aws_sns_topic.pipeline_alerts.arn
    }
  }
}


# ─────────────────────────────────────────────
# SNS Alerts
# ─────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "ecommerce-pipeline-alerts-${var.environment}"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


# ─────────────────────────────────────────────
# AWS Glue: Database + Crawlers + Jobs
# ─────────────────────────────────────────────

resource "aws_glue_catalog_database" "ecommerce" {
  name        = "ecommerce_catalog_${var.environment}"
  description = "Glue Data Catalog for E-Commerce Data Lake"
}

resource "aws_iam_role" "glue_exec" {
  name = "ecommerce-glue-exec-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = ["${aws_s3_bucket.data_lake.arn}", "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

resource "aws_glue_crawler" "silver_orders" {
  database_name = aws_glue_catalog_database.ecommerce.name
  name          = "ecommerce-silver-orders-crawler-${var.environment}"
  role          = aws_iam_role.glue_exec.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/orders/"
  }

  schedule = "cron(0 * * * ? *)"  # Every hour
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "ecommerce-bronze-to-silver-${var.environment}"
  role_arn          = aws_iam_role.glue_exec.arn
  glue_version      = "4.0"
  number_of_workers = var.glue_worker_count
  worker_type       = var.glue_worker_type
  timeout           = 60  # minutes

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/bronze_to_silver/orders_transformation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${aws_s3_bucket.data_lake.bucket}/glue-temp/"
    "--S3_BUCKET"                  = aws_s3_bucket.data_lake.bucket
    "--ENVIRONMENT"                = var.environment
    "--GLUE_DATABASE"              = aws_glue_catalog_database.ecommerce.name
    "--INCREMENTAL_MODE"           = "true"
    "--extra-py-files"             = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/deps/libs.zip"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name              = "ecommerce-silver-to-gold-${var.environment}"
  role_arn          = aws_iam_role.glue_exec.arn
  glue_version      = "4.0"
  number_of_workers = var.glue_worker_count
  worker_type       = var.glue_worker_type
  timeout           = 90

  command {
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/silver_to_gold/gold_aggregations.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option"        = "job-bookmark-enable"
    "--enable-metrics"             = "true"
    "--TempDir"                    = "s3://${aws_s3_bucket.data_lake.bucket}/glue-temp/"
    "--S3_BUCKET"                  = aws_s3_bucket.data_lake.bucket
    "--ENVIRONMENT"                = var.environment
    "--GLUE_DATABASE"              = aws_glue_catalog_database.ecommerce.name
  }
}


# ─────────────────────────────────────────────
# CloudWatch Alarms
# ─────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "ecommerce-orders-lambda-errors-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "Orders Lambda error rate too high"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    FunctionName = aws_lambda_function.orders_processor.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  alarm_name          = "ecommerce-kinesis-iterator-age-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300000  # 5 minutes
  alarm_description   = "Kinesis consumer falling behind"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.streams["orders"].name
  }
}


# ─────────────────────────────────────────────
# Outputs
# ─────────────────────────────────────────────

output "s3_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "Data Lake S3 bucket name"
}

output "kinesis_stream_arns" {
  value       = { for k, v in aws_kinesis_stream.streams : k => v.arn }
  description = "Kinesis stream ARNs"
}

output "sns_alert_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
  description = "SNS alert topic ARN"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.ecommerce.name
  description = "Glue catalog database name"
}
