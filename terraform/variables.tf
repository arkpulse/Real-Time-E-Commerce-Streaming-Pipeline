# =============================================================
# Terraform Variables Definition
# =============================================================

variable "aws_region" {
  type        = string
  description = "AWS region to deploy into"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Deployment environment: dev | staging | prod"
  default     = "prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  type        = string
  description = "Project name prefix for all resources"
  default     = "ecommerce-streaming"
}

variable "kinesis_shard_count" {
  type        = number
  description = "Number of shards per Kinesis stream"
  default     = 4
  validation {
    condition     = var.kinesis_shard_count >= 1 && var.kinesis_shard_count <= 200
    error_message = "Shard count must be between 1 and 200."
  }
}

variable "kinesis_retention_hours" {
  type        = number
  description = "Kinesis stream record retention in hours (24–8760)"
  default     = 24
}

variable "lambda_memory_mb" {
  type        = number
  description = "Lambda function memory in MB"
  default     = 512
}

variable "lambda_timeout_seconds" {
  type        = number
  description = "Lambda timeout in seconds"
  default     = 300
}

variable "lambda_batch_size" {
  type        = number
  description = "Kinesis→Lambda batch size (1–10000)"
  default     = 100
}

variable "lambda_parallelization_factor" {
  type        = number
  description = "Concurrent Lambda invocations per shard (1–10)"
  default     = 4
}

variable "s3_bucket_name" {
  type        = string
  description = "Base name for the S3 data lake bucket"
  default     = "ecommerce-data-lake"
}

variable "s3_bronze_retention_days" {
  type        = number
  description = "Days before Bronze objects transition to STANDARD_IA"
  default     = 30
}

variable "s3_bronze_glacier_days" {
  type        = number
  description = "Days before Bronze objects transition to GLACIER"
  default     = 90
}

variable "s3_bronze_expiry_days" {
  type        = number
  description = "Days before Bronze objects expire"
  default     = 365
}

variable "glue_worker_type" {
  type        = string
  description = "Glue worker type: G.025X | G.1X | G.2X | G.4X | G.8X"
  default     = "G.1X"
}

variable "glue_worker_count" {
  type        = number
  description = "Number of Glue workers"
  default     = 5
}

variable "glue_job_timeout_minutes" {
  type        = number
  description = "Glue job timeout in minutes"
  default     = 60
}

variable "alert_email" {
  type        = string
  description = "Email address for pipeline operational alerts"
}

variable "kinesis_iterator_age_threshold_ms" {
  type        = number
  description = "CloudWatch alarm threshold for Kinesis iterator age (ms)"
  default     = 300000   # 5 minutes
}

variable "lambda_error_threshold" {
  type        = number
  description = "CloudWatch alarm threshold for Lambda error count"
  default     = 10
}

variable "enable_xray" {
  type        = bool
  description = "Enable AWS X-Ray tracing on Lambda functions"
  default     = true
}

variable "tags" {
  type        = map(string)
  description = "Additional resource tags"
  default     = {}
}
