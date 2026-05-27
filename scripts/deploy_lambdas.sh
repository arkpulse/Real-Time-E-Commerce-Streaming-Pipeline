#!/usr/bin/env bash
# =============================================================
# Deploy Script: Package & Update Lambda Functions
# =============================================================
# Packages each Lambda with its dependencies, uploads to S3,
# and updates the function code + publishes a new version.
#
# Usage:
#   ./scripts/deploy_lambdas.sh prod
#   ./scripts/deploy_lambdas.sh staging
#   FUNCTIONS="orders_processor" ./scripts/deploy_lambdas.sh dev
# =============================================================

set -euo pipefail

ENVIRONMENT="${1:-dev}"
FUNCTIONS="${FUNCTIONS:-orders_processor payments_processor clickstream_processor cart_processor dead_letter_handler}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/.build"
PYTHON_VERSION="3.11"

# Load .env if present
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    # shellcheck disable=SC1091
    set -a; source "$PROJECT_ROOT/.env"; set +a
fi

S3_BUCKET="${S3_BUCKET:-ecommerce-data-lake}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo "========================================"
echo "  Lambda Deploy"
echo "  Environment : $ENVIRONMENT"
echo "  Region      : $AWS_REGION"
echo "  S3 Bucket   : $S3_BUCKET"
echo "========================================"

# Verify AWS auth
aws sts get-caller-identity --query Arn --output text >/dev/null || {
    echo "ERROR: AWS credentials not configured"; exit 1
}

rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"

# ── Package each function ──────────────────────────────────

for FUNC in $FUNCTIONS; do
    FUNC_DIR="$PROJECT_ROOT/lambda/$FUNC"
    FUNC_NAME="ecommerce-${FUNC//_/-}-${ENVIRONMENT}"
    ZIP_FILE="$BUILD_DIR/${FUNC}.zip"
    PKG_DIR="$BUILD_DIR/${FUNC}_pkg"

    echo ""
    echo "── Packaging $FUNC ────────────────────────────"

    if [[ ! -d "$FUNC_DIR" ]]; then
        echo "  SKIP: $FUNC_DIR not found"
        continue
    fi

    rm -rf "$PKG_DIR" && mkdir -p "$PKG_DIR"

    # Install dependencies into package dir
    if [[ -f "$FUNC_DIR/requirements.txt" ]]; then
        echo "  Installing dependencies..."
        pip install \
            --quiet \
            --target "$PKG_DIR" \
            --python-version "$PYTHON_VERSION" \
            --platform manylinux2014_x86_64 \
            --only-binary :all: \
            -r "$FUNC_DIR/requirements.txt"
    fi

    # Copy handler code
    cp "$FUNC_DIR/handler.py" "$PKG_DIR/"
    # Copy any shared modules if present
    [[ -f "$FUNC_DIR/utils.py" ]] && cp "$FUNC_DIR/utils.py" "$PKG_DIR/"

    # Zip it up
    echo "  Zipping..."
    cd "$PKG_DIR"
    zip -q -r "$ZIP_FILE" . -x "*.pyc" -x "__pycache__/*" -x "*.dist-info/*"
    cd "$PROJECT_ROOT"

    ZIP_SIZE=$(du -sh "$ZIP_FILE" | cut -f1)
    echo "  Package size: $ZIP_SIZE"

    # Upload to S3
    S3_KEY="lambda-packages/${FUNC}/${ENVIRONMENT}/deployment.zip"
    echo "  Uploading to s3://$S3_BUCKET/$S3_KEY ..."
    aws s3 cp "$ZIP_FILE" "s3://$S3_BUCKET/$S3_KEY" --region "$AWS_REGION"

    # Update Lambda function code
    echo "  Updating Lambda function: $FUNC_NAME ..."
    UPDATE_OUTPUT=$(aws lambda update-function-code \
        --function-name "$FUNC_NAME" \
        --s3-bucket "$S3_BUCKET" \
        --s3-key "$S3_KEY" \
        --region "$AWS_REGION" \
        --output json 2>&1) || {
            echo "  WARNING: Function $FUNC_NAME not found — skipping update"
            continue
        }

    # Wait for update to complete
    aws lambda wait function-updated \
        --function-name "$FUNC_NAME" \
        --region "$AWS_REGION" 2>/dev/null || true

    # Publish a new version (prod only)
    if [[ "$ENVIRONMENT" == "prod" ]]; then
        VERSION=$(aws lambda publish-version \
            --function-name "$FUNC_NAME" \
            --description "Deploy $(git rev-parse --short HEAD 2>/dev/null || echo 'manual')" \
            --region "$AWS_REGION" \
            --query Version --output text)
        echo "  Published version: $VERSION"

        # Update LIVE alias
        aws lambda update-alias \
            --function-name "$FUNC_NAME" \
            --name LIVE \
            --function-version "$VERSION" \
            --region "$AWS_REGION" 2>/dev/null || \
        aws lambda create-alias \
            --function-name "$FUNC_NAME" \
            --name LIVE \
            --function-version "$VERSION" \
            --region "$AWS_REGION" >/dev/null
        echo "  Alias LIVE → v$VERSION"
    fi

    echo "  ✓ $FUNC deployed successfully"
done

# ── Upload Glue scripts ────────────────────────────────────

echo ""
echo "── Uploading Glue scripts ────────────────────────────"
aws s3 sync "$PROJECT_ROOT/glue/" \
    "s3://$S3_BUCKET/scripts/" \
    --exclude "*.pyc" \
    --exclude "__pycache__/*" \
    --region "$AWS_REGION"
echo "  ✓ Glue scripts uploaded"

# ── Cleanup ────────────────────────────────────────────────

rm -rf "$BUILD_DIR"

echo ""
echo "========================================"
echo "  ✅ Deploy complete  ($ENVIRONMENT)"
echo "========================================"
