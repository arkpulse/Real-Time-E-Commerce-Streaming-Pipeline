#!/usr/bin/env bash
# =============================================================
# Glue Pipeline Trigger Script
# =============================================================
# Triggers Bronze→Silver and Silver→Gold Glue jobs
# for a given processing date. Used by schedulers
# (cron, Airflow, Step Functions, etc.) or manually.
#
# Usage:
#   ./scripts/run_glue_pipeline.sh prod 2025-06-01
#   ./scripts/run_glue_pipeline.sh dev   # defaults to today
# =============================================================

set -euo pipefail

ENVIRONMENT="${1:-dev}"
PROCESSING_DATE="${2:-$(date -u +%Y-%m-%d)}"
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-ecommerce-data-lake}"
GLUE_DATABASE="${GLUE_DATABASE:-ecommerce_catalog}"

BRONZE_SILVER_JOB="ecommerce-bronze-to-silver-${ENVIRONMENT}"
SILVER_GOLD_JOB="ecommerce-silver-to-gold-${ENVIRONMENT}"

echo "========================================"
echo "  Glue Pipeline Run"
echo "  Environment     : $ENVIRONMENT"
echo "  Processing Date : $PROCESSING_DATE"
echo "========================================"

# ── Helper: start and poll a Glue job ─────────────────────

run_glue_job() {
    local JOB_NAME="$1"
    local EXTRA_ARGS="$2"
    local POLL_INTERVAL=30

    echo ""
    echo "Starting: $JOB_NAME"
    RUN_ID=$(aws glue start-job-run \
        --job-name "$JOB_NAME" \
        --arguments "$EXTRA_ARGS" \
        --region "$AWS_REGION" \
        --query JobRunId --output text)

    echo "  Run ID: $RUN_ID"
    echo "  Polling every ${POLL_INTERVAL}s..."

    while true; do
        STATUS=$(aws glue get-job-run \
            --job-name "$JOB_NAME" \
            --run-id "$RUN_ID" \
            --region "$AWS_REGION" \
            --query JobRun.JobRunState \
            --output text)

        echo "  Status: $STATUS  ($(date -u +%H:%M:%S))"

        case "$STATUS" in
            SUCCEEDED)
                echo "  ✓ $JOB_NAME SUCCEEDED"
                return 0
                ;;
            FAILED|ERROR|TIMEOUT|STOPPED)
                echo "  ✗ $JOB_NAME FAILED with status: $STATUS"
                # Print last 20 lines of error message
                aws glue get-job-run \
                    --job-name "$JOB_NAME" \
                    --run-id "$RUN_ID" \
                    --region "$AWS_REGION" \
                    --query JobRun.ErrorMessage \
                    --output text | head -20
                return 1
                ;;
            RUNNING|STARTING|STOPPING)
                sleep "$POLL_INTERVAL"
                ;;
            *)
                echo "  Unknown status: $STATUS — waiting..."
                sleep "$POLL_INTERVAL"
                ;;
        esac
    done
}

# ── Common job arguments ─────────────────────────────────

COMMON_ARGS=$(cat <<EOF
{
  "--S3_BUCKET": "$S3_BUCKET",
  "--ENVIRONMENT": "$ENVIRONMENT",
  "--GLUE_DATABASE": "$GLUE_DATABASE",
  "--PROCESSING_DATE": "$PROCESSING_DATE",
  "--INCREMENTAL_MODE": "true",
  "--BRONZE_PREFIX": "bronze",
  "--SILVER_PREFIX": "silver",
  "--GOLD_PREFIX": "gold"
}
EOF
)

# ── Step 1: Bronze → Silver ───────────────────────────────

echo "STEP 1: Bronze → Silver"
if run_glue_job "$BRONZE_SILVER_JOB" "$COMMON_ARGS"; then
    echo "  Bronze→Silver complete ✓"
else
    echo "ERROR: Bronze→Silver failed — aborting pipeline"
    exit 1
fi

# ── Step 2: Silver → Gold ─────────────────────────────────

echo "STEP 2: Silver → Gold"
if run_glue_job "$SILVER_GOLD_JOB" "$COMMON_ARGS"; then
    echo "  Silver→Gold complete ✓"
else
    echo "ERROR: Silver→Gold failed"
    exit 1
fi

echo ""
echo "========================================"
echo "  ✅ Pipeline complete for $PROCESSING_DATE"
echo "========================================"
