# =============================================================
# Terraform Outputs
# =============================================================

output "s3_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "S3 data lake bucket name"
}

output "s3_bucket_arn" {
  value       = aws_s3_bucket.data_lake.arn
  description = "S3 data lake bucket ARN"
}

output "kinesis_stream_arns" {
  value       = { for k, v in aws_kinesis_stream.streams : k => v.arn }
  description = "Map of stream name → ARN"
}

output "kinesis_stream_names" {
  value       = { for k, v in aws_kinesis_stream.streams : k => v.name }
  description = "Map of stream key → stream name"
}

output "lambda_orders_arn" {
  value       = aws_lambda_function.orders_processor.arn
  description = "Orders processor Lambda ARN"
}

output "lambda_role_arn" {
  value       = aws_iam_role.lambda_exec.arn
  description = "Lambda execution role ARN"
}

output "glue_role_arn" {
  value       = aws_iam_role.glue_exec.arn
  description = "Glue execution role ARN"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.ecommerce.name
  description = "Glue catalog database name"
}

output "glue_bronze_to_silver_job" {
  value       = aws_glue_job.bronze_to_silver.name
  description = "Bronze-to-Silver Glue job name"
}

output "glue_silver_to_gold_job" {
  value       = aws_glue_job.silver_to_gold.name
  description = "Silver-to-Gold Glue job name"
}

output "sns_alert_topic_arn" {
  value       = aws_sns_topic.pipeline_alerts.arn
  description = "SNS pipeline alerts topic ARN"
}

output "dynamodb_dedup_orders_table" {
  value       = aws_dynamodb_table.dedup_orders.name
  description = "DynamoDB dedup table for orders"
}

output "dynamodb_dedup_payments_table" {
  value       = aws_dynamodb_table.dedup_payments.name
  description = "DynamoDB dedup table for payments"
}

output "kms_kinesis_key_arn" {
  value       = aws_kms_key.kinesis.arn
  description = "KMS key ARN used for Kinesis encryption"
}

output "environment" {
  value       = var.environment
  description = "Deployed environment"
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS region"
}

# ── .env snippet (copy into .env file) ────────────────────────

output "env_snippet" {
  description = "Paste into .env file"
  value = <<-EOT
    AWS_REGION=${var.aws_region}
    S3_BUCKET=${aws_s3_bucket.data_lake.bucket}
    ORDERS_STREAM=${aws_kinesis_stream.streams["orders"].name}
    PAYMENTS_STREAM=${aws_kinesis_stream.streams["payments"].name}
    CLICKSTREAM_STREAM=${aws_kinesis_stream.streams["clickstream"].name}
    CART_STREAM=${aws_kinesis_stream.streams["cart"].name}
    SNS_TOPIC_ARN=${aws_sns_topic.pipeline_alerts.arn}
    DEDUP_TABLE_ORDERS=${aws_dynamodb_table.dedup_orders.name}
    DEDUP_TABLE_PAYMENTS=${aws_dynamodb_table.dedup_payments.name}
    GLUE_DATABASE=${aws_glue_catalog_database.ecommerce.name}
  EOT
  sensitive = false
}
