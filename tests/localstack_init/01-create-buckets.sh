#!/bin/bash
# This script is automatically executed by LocalStack on startup
# It creates test S3 buckets for integration testing

echo "Creating test S3 buckets..."

# Create test buckets
awslocal s3 mb s3://queuepipeio-test-bucket
awslocal s3 mb s3://queuepipeio-input-bucket
awslocal s3 mb s3://queuepipeio-output-bucket

# List buckets to confirm creation
echo "Created S3 buckets:"
awslocal s3 ls

echo "S3 bucket initialization complete!"