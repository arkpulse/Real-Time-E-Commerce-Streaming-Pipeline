#!/usr/bin/env bash
# =============================================================
# LocalStack Initialization Script
# =============================================================
# Automatically provisions all AWS resources in LocalStack
# for local development and integration testing.
#
# Runs automatically when LocalStack starts via the
# /etc/localstack/init/ready.d/ hook.
# =============================================================

set -euo pipefail

AWS="aws --endpoint-url=http://localhost:4566 --region us-east-1 \
    --no-sign-request \
    --output json"

BUCKET="ecommerce-data-lake"
ENVIRONMENT="local"

echo "============================================"
echo " LocalStack: Initializing E-Commerce Stack"
echo "============================================"


# ── S3 Data Lake ─────────────────────────────────────────────

echo "[S3] Creating data lake bucket..."
$AWS s3 mb s3://$BUCKET 2>/dev/null || echo "  Bucket already exists"

# Create folder structure with placeholder files
for prefix in bronze/orders bronze/payments bronze/clickstream bronze/carts \
              silver/orders silver/payments silver/clickstream silver/carts \
              gold/fact_orders gold/fact_payments gold/fact_clickstream \
              gold/dim_users gold/dim_products gold/dim_time \
              dlq/orders dlq/payments dlq/clickstream \
              glue-temp scripts lambda-packages athena-results; do
    echo "" | $AWS s3 cp - s3://$BUCKET/$prefix/.keep 2>/dev/null || true
done
echo "  ✓ S3 bucket and folder structure created"


# ── Kinesis Streams ────────────────────────────────────────

echo "[Kinesis] Creating data streams..."
for stream in orders payments clickstream cart dlq; do
    STREAM_NAME="ecommerce-${stream}-stream"
    $AWS kinesis create-stream \
        --stream-name "$STREAM_NAME" \
        --shard-count 2 2>/dev/null || echo "  Stream $STREAM_NAME already exists"
done
echo "  ✓ Kinesis streams created"


# ── DynamoDB (Dedup Tables) ────────────────────────────────

echo "[DynamoDB] Creating dedup tables..."

$AWS dynamodb create-table \
    --table-name ecommerce-dedup-orders \
    --attribute-definitions AttributeName=order_id,AttributeType=S \
    --key-schema AttributeName=order_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --table-class STANDARD 2>/dev/null || echo "  Table already exists"

$AWS dynamodb create-table \
    --table-name ecommerce-dedup-payments \
    --attribute-definitions AttributeName=payment_id,AttributeType=S \
    --key-schema AttributeName=payment_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --table-class STANDARD 2>/dev/null || echo "  Table already exists"

echo "  ✓ DynamoDB tables created"


# ── SNS Topics ─────────────────────────────────────────────

echo "[SNS] Creating alert topics..."

ALERT_ARN=$($AWS sns create-topic \
    --name ecommerce-pipeline-alerts \
    --query TopicArn --output text)

ABANDONMENT_ARN=$($AWS sns create-topic \
    --name ecommerce-cart-abandonment \
    --query TopicArn --output text)

echo "  ✓ SNS topics created"
echo "    Alert ARN:       $ALERT_ARN"
echo "    Abandonment ARN: $ABANDONMENT_ARN"


# ── Lambda Functions ───────────────────────────────────────

echo "[Lambda] Creating Lambda functions..."

# Create a simple placeholder zip if no real deployment package exists
PLACEHOLDER_HANDLER=$(mktemp -d)
cat > "$PLACEHOLDER_HANDLER/handler.py" << 'EOF'
import json
def lambda_handler(event, context):
    print(f"Received {len(event.get('Records', []))} records")
    return {"statusCode": 200, "message": "placeholder"}
EOF

cd "$PLACEHOLDER_HANDLER"
zip -q handler.zip handler.py
cd -

for func in orders payments clickstream cart; do
    FUNC_NAME="ecommerce-${func}-processor"
    $AWS lambda create-function \
        --function-name "$FUNC_NAME" \
        --runtime python3.11 \
        --role arn:aws:iam::000000000000:role/lambda-role \
        --handler handler.lambda_handler \
        --zip-file "fileb://$PLACEHOLDER_HANDLER/handler.zip" \
        --environment "Variables={S3_BUCKET=$BUCKET,ENVIRONMENT=$ENVIRONMENT,LOG_LEVEL=DEBUG}" \
        --timeout 300 \
        --memory-size 512 2>/dev/null || echo "  Function $FUNC_NAME already exists"
done

rm -rf "$PLACEHOLDER_HANDLER"
echo "  ✓ Lambda functions created"


# ── Event Source Mappings (Kinesis → Lambda) ──────────────

echo "[ESM] Creating Kinesis→Lambda event source mappings..."

STREAMS=("orders" "payments" "clickstream" "cart")
LAMBDAS=("orders-processor" "payments-processor" "clickstream-processor" "cart-processor")

for i in "${!STREAMS[@]}"; do
    STREAM_ARN=$($AWS kinesis describe-stream \
        --stream-name "ecommerce-${STREAMS[$i]}-stream" \
        --query StreamDescription.StreamARN --output text)

    $AWS lambda create-event-source-mapping \
        --event-source-arn "$STREAM_ARN" \
        --function-name "ecommerce-${LAMBDAS[$i]}" \
        --starting-position LATEST \
        --batch-size 100 2>/dev/null || echo "  Mapping already exists for ${STREAMS[$i]}"
done
echo "  ✓ Event source mappings created"


# ── Glue Database ──────────────────────────────────────────

echo "[Glue] Creating Glue catalog database..."
$AWS glue create-database \
    --database-input '{"Name":"ecommerce_catalog","Description":"E-Commerce Data Lake Catalog"}' \
    2>/dev/null || echo "  Database already exists"
echo "  ✓ Glue database created"


# ── CloudWatch Log Groups ────────────────────────────────

echo "[CloudWatch] Creating log groups..."
for func in orders payments clickstream cart; do
    $AWS logs create-log-group \
        --log-group-name "/aws/lambda/ecommerce-${func}-processor" \
        2>/dev/null || true
done
echo "  ✓ CloudWatch log groups created"


# ── Print Summary ──────────────────────────────────────────

echo ""
echo "============================================"
echo " ✅ LocalStack Initialization Complete!"
echo "============================================"
echo ""
echo "Resources created:"
echo "  S3 Bucket:    s3://$BUCKET"
echo "  Kinesis:      5 streams (orders, payments, clickstream, cart, dlq)"
echo "  DynamoDB:     2 dedup tables"
echo "  SNS:          2 topics"
echo "  Lambda:       4 functions"
echo "  Glue DB:      ecommerce_catalog"
echo ""
echo "To test producer locally against LocalStack:"
echo "  export AWS_ENDPOINT_URL=http://localhost:4566"
echo "  export AWS_DEFAULT_REGION=us-east-1"
echo "  export AWS_ACCESS_KEY_ID=test"
echo "  export AWS_SECRET_ACCESS_KEY=test"
echo "  cd producer && python main.py"
echo ""
