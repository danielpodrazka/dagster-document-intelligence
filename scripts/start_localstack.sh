#!/bin/bash
set -e

BUCKET="dagster-document-intelligence-etl"
ENDPOINT="http://localhost:4566"

echo "Starting LocalStack..."
docker run -d -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack

echo "Waiting for LocalStack to be ready..."
sleep 2

echo "Creating S3 bucket: $BUCKET"
aws --endpoint-url="$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true

echo "LocalStack ready. Bucket: $BUCKET"
