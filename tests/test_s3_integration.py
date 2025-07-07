#!/usr/bin/env python3
"""
S3 integration tests using LocalStack for the new pipe architecture.

These tests verify that QueuePipeIO works correctly with S3 operations
including uploads, downloads, and hash verification.

LocalStack is automatically started/stopped for these tests.
"""

import unittest
import threading
import hashlib
import time
import os
import sys
import subprocess
import atexit
from unittest import skipUnless

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, HashingFilter

# Try to import boto3
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    boto3 = None
    ClientError = None
    BOTO3_AVAILABLE = False

MB = 1024 * 1024


class LocalStackManager:
    """Manages LocalStack lifecycle for tests."""
    _instance = None
    _started = False
    _process = None
    
    # Environment variable to keep LocalStack running
    KEEP_RUNNING = os.environ.get('KEEP_LOCALSTACK', '').lower() in ('1', 'true', 'yes')
    
    @classmethod
    def start(cls):
        """Start LocalStack if not already running."""
        if cls._started or cls.is_running():
            return True
        
        print("\n🚀 Starting LocalStack for S3 tests...")
        try:
            # Start LocalStack using docker-compose
            result = subprocess.run(
                ["docker-compose", "up", "-d", "localstack"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"Error: {result.stderr}")
                return False
            
            # Wait for LocalStack to be ready
            max_retries = 30
            for i in range(max_retries):
                time.sleep(1)
                if cls.is_running():
                    cls._started = True
                    print("✅ LocalStack started successfully")
                    
                    # Ensure test bucket exists
                    cls._create_test_bucket()
                    return True
                
                if i % 5 == 0:
                    print(f"   Waiting for LocalStack to start... ({i}/{max_retries})")
            
            print("❌ LocalStack failed to start")
            return False
            
        except Exception as e:
            print(f"❌ Error starting LocalStack: {e}")
            return False
    
    @classmethod
    def stop(cls):
        """Stop LocalStack if we started it."""
        if not cls._started:
            return
        
        if cls.KEEP_RUNNING:
            print("\n🟡 Keeping LocalStack running (KEEP_LOCALSTACK=true)")
            return
        
        print("\n🛑 Stopping LocalStack...")
        try:
            subprocess.run(
                ["docker-compose", "stop", "localstack"],
                capture_output=True,
                check=True
            )
            cls._started = False
            print("✅ LocalStack stopped")
        except Exception as e:
            print(f"❌ Error stopping LocalStack: {e}")
    
    @classmethod
    def is_running(cls):
        """Check if LocalStack is running and healthy."""
        try:
            result = subprocess.run(
                ["curl", "-f", "http://localhost:4566/_localstack/health"],
                capture_output=True,
                timeout=2
            )
            return result.returncode == 0
        except Exception:
            return False
    
    @classmethod
    def _create_test_bucket(cls):
        """Ensure test bucket exists."""
        if not BOTO3_AVAILABLE:
            return
        
        try:
            s3 = create_s3_client()
            s3.create_bucket(Bucket=S3Config.TEST_BUCKET)
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyExists':
                print(f"Warning: Could not create test bucket: {e}")


# Register cleanup at exit
def cleanup_localstack():
    """Cleanup function to ensure LocalStack is stopped."""
    if LocalStackManager._started:
        LocalStackManager.stop()

atexit.register(cleanup_localstack)


class S3Config:
    """LocalStack S3 configuration."""
    ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    REGION = "us-east-1"
    ACCESS_KEY = "test"
    SECRET_KEY = "test"
    TEST_BUCKET = "queuepipeio-test-bucket"


def create_s3_client():
    """Create S3 client configured for LocalStack."""
    if not BOTO3_AVAILABLE:
        raise ImportError("boto3 is required for S3 tests. Install with: pip install boto3")
    
    return boto3.client(
        "s3",
        endpoint_url=S3Config.ENDPOINT_URL,
        region_name=S3Config.REGION,
        aws_access_key_id=S3Config.ACCESS_KEY,
        aws_secret_access_key=S3Config.SECRET_KEY,
    )


def localstack_available():
    """Check if LocalStack is available (will start it if needed)."""
    if not BOTO3_AVAILABLE:
        return False
    
    # Try to start LocalStack if not running
    if not LocalStackManager.is_running():
        if not LocalStackManager.start():
            return False
    
    try:
        s3 = create_s3_client()
        s3.list_buckets()
        return True
    except Exception:
        return False


@skipUnless(BOTO3_AVAILABLE, "boto3 not available")
class TestS3PipeIntegration(unittest.TestCase):
    """Test S3 operations with the new pipe architecture."""
    
    @classmethod
    def setUpClass(cls):
        """Set up S3 client and ensure LocalStack is running."""
        # Start LocalStack if needed
        if not localstack_available():
            raise unittest.SkipTest("Could not start LocalStack")
        
        cls.s3 = create_s3_client()
        
        # Ensure test bucket exists
        try:
            cls.s3.create_bucket(Bucket=S3Config.TEST_BUCKET)
        except ClientError as e:
            if e.response['Error']['Code'] != 'BucketAlreadyExists':
                raise
    
    def setUp(self):
        """Set up for each test."""
        self.test_keys = []
    
    def tearDown(self):
        """Clean up test objects."""
        for key in self.test_keys:
            try:
                self.s3.delete_object(Bucket=S3Config.TEST_BUCKET, Key=key)
            except Exception:
                pass
    
    def test_simple_s3_upload_download(self):
        """Test basic S3 upload and download using pipes."""
        test_key = "test-simple-upload.bin"
        self.test_keys.append(test_key)
        
        # Generate test data
        test_data = b"Hello from S3!" * 1000
        
        # Upload using pipe
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        # Upload thread
        def upload_to_s3():
            # Read from pipe and upload to S3
            data = reader.read()
            self.s3.put_object(
                Bucket=S3Config.TEST_BUCKET,
                Key=test_key,
                Body=data
            )
        
        upload_thread = threading.Thread(target=upload_to_s3)
        upload_thread.start()
        
        # Write data to pipe
        writer.write(test_data)
        writer.close()
        
        upload_thread.join()
        
        # Download and verify
        response = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=test_key)
        downloaded_data = response['Body'].read()
        
        self.assertEqual(downloaded_data, test_data)
    
    def test_streaming_upload_with_hash(self):
        """Test streaming upload with hash computation."""
        test_key = "test-streaming-upload.bin"
        self.test_keys.append(test_key)
        
        # Generate larger test data (need at least 5MB for multipart)
        test_data = os.urandom(20 * MB)
        expected_hash = hashlib.sha256(test_data).hexdigest()
        
        # Create pipeline: writer -> hasher -> reader
        writer = PipeWriter(memory_limit=2*MB, chunk_size=512*1024)
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        # S3 upload thread
        upload_complete = threading.Event()
        
        def upload_to_s3():
            # Use multipart upload for larger file
            upload_id = self.s3.create_multipart_upload(
                Bucket=S3Config.TEST_BUCKET,
                Key=test_key
            )['UploadId']
            
            parts = []
            part_number = 1
            
            try:
                while True:
                    # Read 5MB chunks for multipart (S3 minimum)
                    chunk = reader.read(5 * MB)
                    if not chunk:
                        break
                    
                    # Upload part
                    response = self.s3.upload_part(
                        Bucket=S3Config.TEST_BUCKET,
                        Key=test_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    
                    parts.append({
                        'ETag': response['ETag'],
                        'PartNumber': part_number
                    })
                    part_number += 1
                
                # Complete multipart upload
                self.s3.complete_multipart_upload(
                    Bucket=S3Config.TEST_BUCKET,
                    Key=test_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                upload_complete.set()
                
            except Exception:
                self.s3.abort_multipart_upload(
                    Bucket=S3Config.TEST_BUCKET,
                    Key=test_key,
                    UploadId=upload_id
                )
                raise
        
        # Start upload thread
        upload_thread = threading.Thread(target=upload_to_s3)
        upload_thread.start()
        
        # Write data in chunks
        chunk_size = 256 * 1024
        for i in range(0, len(test_data), chunk_size):
            chunk = test_data[i:i+chunk_size]
            writer.write(chunk)
            time.sleep(0.0001)  # Simulate data generation
        
        writer.close()
        
        # Wait for upload to complete
        upload_thread.join()
        self.assertTrue(upload_complete.is_set())
        
        # Verify hash
        computed_hash = hasher.get_hash()
        self.assertEqual(computed_hash, expected_hash)
        self.assertEqual(hasher.get_bytes_hashed(), len(test_data))
        
        # Download and verify data integrity
        response = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=test_key)
        downloaded_data = response['Body'].read()
        self.assertEqual(len(downloaded_data), len(test_data))
        self.assertEqual(hashlib.sha256(downloaded_data).hexdigest(), expected_hash)
    
    def test_s3_download_with_hash_verification(self):
        """Test downloading from S3 with hash verification."""
        test_key = "test-download-verify.bin"
        self.test_keys.append(test_key)
        
        # Generate and upload test data
        test_data = os.urandom(3 * MB)
        expected_hash = hashlib.sha256(test_data).hexdigest()
        
        # Upload test data with hash in metadata
        self.s3.put_object(
            Bucket=S3Config.TEST_BUCKET,
            Key=test_key,
            Body=test_data,
            Metadata={'sha256': expected_hash}
        )
        
        # Create pipeline for download: writer -> hasher -> reader
        writer = PipeWriter(memory_limit=1*MB)
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        # Download thread
        def download_from_s3():
            response = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=test_key)
            body = response['Body']
            expected = response['Metadata'].get('sha256')
            
            # Stream download to pipe
            while True:
                chunk = body.read(256 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
            
            writer.close()
            return expected
        
        # Storage thread
        downloaded_data = []
        def store_locally():
            while True:
                chunk = reader.read(128 * 1024)
                if not chunk:
                    break
                downloaded_data.append(chunk)
        
        # Run download and storage
        download_thread = threading.Thread(target=download_from_s3)
        storage_thread = threading.Thread(target=store_locally)
        
        download_thread.start()
        storage_thread.start()
        
        download_thread.join()
        storage_thread.join()
        
        # Verify
        result_data = b"".join(downloaded_data)
        self.assertEqual(len(result_data), len(test_data))
        
        # Verify hash
        computed_hash = hasher.get_hash()
        self.assertEqual(computed_hash, expected_hash)
        self.assertEqual(hasher.get_bytes_hashed(), len(test_data))
    
    def test_concurrent_s3_operations(self):
        """Test multiple concurrent S3 operations."""
        num_files = 3
        file_size = 1 * MB
        
        results = []
        
        def process_file(index):
            test_key = f"test-concurrent-{index}.bin"
            self.test_keys.append(test_key)
            
            # Generate unique data
            test_data = os.urandom(file_size)
            expected_hash = hashlib.sha256(test_data).hexdigest()
            
            # Create pipeline
            writer = PipeWriter()
            hasher = HashingFilter()
            reader = PipeReader()
            
            writer | hasher | reader
            
            # Upload thread
            def upload():
                data = reader.read()
                self.s3.put_object(
                    Bucket=S3Config.TEST_BUCKET,
                    Key=test_key,
                    Body=data,
                    Metadata={'sha256': expected_hash}
                )
            
            upload_thread = threading.Thread(target=upload)
            upload_thread.start()
            
            # Write data
            writer.write(test_data)
            writer.close()
            
            upload_thread.join()
            
            # Record result
            results.append({
                'key': test_key,
                'expected_hash': expected_hash,
                'computed_hash': hasher.get_hash(),
                'size': file_size
            })
        
        # Process files concurrently
        threads = []
        for i in range(num_files):
            thread = threading.Thread(target=process_file, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all to complete
        for thread in threads:
            thread.join()
        
        # Verify all uploads
        self.assertEqual(len(results), num_files)
        
        for result in results:
            # Verify hash computation
            self.assertEqual(result['computed_hash'], result['expected_hash'])
            
            # Verify S3 object
            response = self.s3.get_object(
                Bucket=S3Config.TEST_BUCKET,
                Key=result['key']
            )
            self.assertEqual(response['Metadata']['sha256'], result['expected_hash'])
            self.assertEqual(response['ContentLength'], result['size'])
    
    def test_s3_copy_with_transformation(self):
        """Test copying S3 object with hash computation."""
        source_key = "test-copy-source.bin"
        dest_key = "test-copy-dest.bin"
        self.test_keys.extend([source_key, dest_key])
        
        # Upload source object
        test_data = os.urandom(2 * MB)
        self.s3.put_object(
            Bucket=S3Config.TEST_BUCKET,
            Key=source_key,
            Body=test_data
        )
        
        # Create pipeline for copy operation
        writer = PipeWriter(memory_limit=1*MB)
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        # Download from source
        def download_source():
            response = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=source_key)
            body = response['Body']
            
            while True:
                chunk = body.read(256 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
            
            writer.close()
        
        # Upload to destination
        def upload_dest():
            data_chunks = []
            while True:
                chunk = reader.read(512 * 1024)
                if not chunk:
                    break
                data_chunks.append(chunk)
            
            data = b"".join(data_chunks)
            computed_hash = hasher.get_hash()
            
            self.s3.put_object(
                Bucket=S3Config.TEST_BUCKET,
                Key=dest_key,
                Body=data,
                Metadata={'sha256': computed_hash}
            )
            
            return computed_hash
        
        # Run copy operation
        download_thread = threading.Thread(target=download_source)
        upload_thread = threading.Thread(target=upload_dest)
        
        download_thread.start()
        upload_thread.start()
        
        download_thread.join()
        upload_thread.join()
        
        # Verify copy
        source_obj = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=source_key)
        dest_obj = self.s3.get_object(Bucket=S3Config.TEST_BUCKET, Key=dest_key)
        
        self.assertEqual(source_obj['ContentLength'], dest_obj['ContentLength'])
        
        # Verify hash was computed correctly
        expected_hash = hashlib.sha256(test_data).hexdigest()
        self.assertEqual(hasher.get_hash(), expected_hash)
        self.assertEqual(dest_obj['Metadata']['sha256'], expected_hash)


@skipUnless(BOTO3_AVAILABLE, "boto3 not available")
class TestS3Performance(unittest.TestCase):
    """Performance tests for S3 operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up S3 client and ensure LocalStack is running."""
        # Start LocalStack if needed
        if not localstack_available():
            raise unittest.SkipTest("Could not start LocalStack")
        
        cls.s3 = create_s3_client()
    
    def test_large_file_performance(self):
        """Test performance with large file transfers."""
        test_key = "test-performance-large.bin"
        file_size = 10 * MB
        
        # Generate test data
        test_data = os.urandom(file_size)
        
        # Create pipeline with memory limit
        writer = PipeWriter(memory_limit=5*MB, chunk_size=1*MB)
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        # Measure upload time
        start_time = time.time()
        
        # Upload thread
        upload_complete = threading.Event()
        
        def upload_worker():
            data_chunks = []
            while True:
                chunk = reader.read(2 * MB)
                if not chunk:
                    break
                data_chunks.append(chunk)
            
            # Single PUT for performance test
            self.s3.put_object(
                Bucket=S3Config.TEST_BUCKET,
                Key=test_key,
                Body=b"".join(data_chunks)
            )
            upload_complete.set()
        
        upload_thread = threading.Thread(target=upload_worker)
        upload_thread.start()
        
        # Write data
        writer.write(test_data)
        writer.close()
        
        upload_thread.join()
        upload_time = time.time() - start_time
        
        # Calculate throughput
        throughput_mbps = (file_size / MB) / upload_time
        print(f"\nUpload throughput: {throughput_mbps:.2f} MB/s")
        
        # Verify hash
        expected_hash = hashlib.sha256(test_data).hexdigest()
        self.assertEqual(hasher.get_hash(), expected_hash)
        
        # Clean up
        try:
            self.s3.delete_object(Bucket=S3Config.TEST_BUCKET, Key=test_key)
        except Exception:
            pass
        
        # Performance assertion (at least 10 MB/s with LocalStack)
        self.assertGreater(throughput_mbps, 10.0)


if __name__ == "__main__":
    # LocalStack will be started automatically by the tests
    unittest.main()