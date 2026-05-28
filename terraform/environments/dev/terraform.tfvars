# =============================================================
# Dev environment variable overrides
# Usage: terraform apply -var-file=environments/dev/terraform.tfvars
# =============================================================

environment                        = "dev"
kinesis_shard_count                = 1
kinesis_retention_hours            = 24
lambda_memory_mb                   = 256
lambda_timeout_seconds             = 120
lambda_batch_size                  = 25
lambda_parallelization_factor      = 1
glue_worker_type                   = "G.025X"
glue_worker_count                  = 2
glue_job_timeout_minutes           = 30
s3_bronze_retention_days           = 7
s3_bronze_glacier_days             = 30
s3_bronze_expiry_days              = 90
kinesis_iterator_age_threshold_ms  = 600000   # 10 min (more lenient in dev)
lambda_error_threshold             = 50
enable_xray                        = false
