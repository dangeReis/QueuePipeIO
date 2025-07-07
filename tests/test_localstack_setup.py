#!/usr/bin/env python3
"""Test script to verify LocalStack S3 setup is working correctly."""

import boto3
import sys


def test_localstack_connection():
    """Test that we can connect to LocalStack and perform basic S3 operations."""
    print("Testing LocalStack S3 connection...")

    # Create S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    try:
        # List buckets
        response = s3.list_buckets()
        print(f"Found {len(response['Buckets'])} buckets:")
        for bucket in response["Buckets"]:
            print(f"  - {bucket['Name']}")

        # Test bucket should exist from initialization script
        expected_buckets = [
            "queuepipeio-test-bucket",
            "queuepipeio-input-bucket",
            "queuepipeio-output-bucket",
        ]

        bucket_names = [b["Name"] for b in response["Buckets"]]
        for expected in expected_buckets:
            if expected in bucket_names:
                print(f"✓ Found expected bucket: {expected}")
            else:
                print(f"✗ Missing expected bucket: {expected}")
                return False

        # Try creating and deleting a test object
        test_bucket = "queuepipeio-test-bucket"
        test_key = "test-object.txt"
        test_content = b"Hello from LocalStack!"

        print(f"\nTesting object operations in {test_bucket}...")

        # Put object
        s3.put_object(Bucket=test_bucket, Key=test_key, Body=test_content)
        print(f"✓ Successfully uploaded {test_key}")

        # Get object
        response = s3.get_object(Bucket=test_bucket, Key=test_key)
        content = response["Body"].read()
        if content == test_content:
            print(f"✓ Successfully retrieved {test_key} with correct content")
        else:
            print("✗ Retrieved content does not match")
            return False

        # Delete object
        s3.delete_object(Bucket=test_bucket, Key=test_key)
        print(f"✓ Successfully deleted {test_key}")

        print("\n✅ LocalStack S3 is working correctly!")
        return True

    except Exception as e:
        print(f"\n❌ Error testing LocalStack: {e}")
        print("\nMake sure LocalStack is running with: make localstack-start")
        return False


if __name__ == "__main__":
    success = test_localstack_connection()
    sys.exit(0 if success else 1)
