# =============================================================
# Production environment variable overrides
# Usage: terraform apply -var-file=environments/prod/terraform.tfvars
# =============================================================

environment                        = "prod"
kinesis_shard_count                = 4
kinesis_retention_hours            = 48
lambda_memory_mb                   = 512
lambda_timeout_seconds             = 300
lambda_batch_size                  = 100
lambda_parallelization_factor      = 4
glue_worker_type                   = "G.1X"
glue_worker_count                  = 5
glue_job_timeout_minutes           = 60
s3_bronze_retention_days           = 30
s3_bronze_glacier_days             = 90
s3_bronze_expiry_days              = 365
kinesis_iterator_age_threshold_ms  = 300000   # 5 min
lambda_error_threshold             = 10
enable_xray                        = true
