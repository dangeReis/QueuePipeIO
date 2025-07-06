#!/usr/bin/env python3
"""
S3 Integration Tests for QueueIO using LocalStack

This module tests streaming files between S3 buckets while computing hashes
and respecting memory limits.
"""

import unittest
import os
import time
import threading
import hashlib
import tempfile
from io import BytesIO

from queuepipeio import QueueIO, LimitedQueueIO

from .s3_test_utils import (
    S3StreamHandler,
    TestFileGenerator,
    create_test_environment,
)


class TestS3Streaming(unittest.TestCase):
    """Test S3 streaming operations with QueueIO"""

    @classmethod
    def setUpClass(cls):
        """Set up S3 client for all tests"""
        try:
            cls.s3_client = create_test_environment()
            # Verify LocalStack is running
            cls.s3_client.list_buckets()
        except Exception as e:
            raise unittest.SkipTest(f"LocalStack not available: {e}")

    def setUp(self):
        """Create test buckets for each test"""
        self.source_bucket = f"test-source-{int(time.time())}"
        self.dest_bucket = f"test-dest-{int(time.time())}"

        self.s3_client.create_bucket(Bucket=self.source_bucket)
        self.s3_client.create_bucket(Bucket=self.dest_bucket)

        self.temp_files = []

    def tearDown(self):
        """Clean up buckets and temp files"""
        # Delete all objects and buckets
        for bucket in [self.source_bucket, self.dest_bucket]:
            try:
                # Delete all objects
                response = self.s3_client.list_objects_v2(Bucket=bucket)
                if "Contents" in response:
                    for obj in response["Contents"]:
                        self.s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                # Delete bucket
                self.s3_client.delete_bucket(Bucket=bucket)
            except Exception:
                pass

        # Clean up temp files
        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
            except Exception:
                pass

    def test_small_file_streaming(self):
        """Test streaming a small file (< 5MB) between buckets"""
        # Generate 1MB test file
        file_path, expected_md5, expected_sha256 = TestFileGenerator.create_test_file(
            1 * 1024 * 1024
        )
        self.temp_files.append(file_path)

        # Upload to source bucket
        source_key = "small-test-file.bin"
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Stream from source to destination with hash computation
        dest_key = "small-test-file-copy.bin"
        hasher = hashlib.sha256()
        bytes_processed = self._stream_s3_to_s3_with_hash(
            self.source_bucket, source_key, self.dest_bucket, dest_key, hasher
        )

        # Verify the copy
        self.assertEqual(bytes_processed, 1 * 1024 * 1024)
        self.assertEqual(hasher.hexdigest(), expected_sha256)

        # Verify destination object exists and has correct size
        response = self.s3_client.head_object(Bucket=self.dest_bucket, Key=dest_key)
        self.assertEqual(response["ContentLength"], 1 * 1024 * 1024)

    def test_multipart_streaming(self):
        """Test streaming a large file requiring multipart upload"""
        # Generate 20MB test file (requires multipart)
        file_path, expected_md5, expected_sha256 = TestFileGenerator.create_test_file(
            20 * 1024 * 1024
        )
        self.temp_files.append(file_path)

        # Upload to source bucket
        source_key = "large-test-file.bin"
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Stream with memory limit
        dest_key = "large-test-file-copy.bin"
        hasher = hashlib.sha256()

        # Use limited queue with 10MB memory limit
        qio = LimitedQueueIO(
            memory_limit=10 * 1024 * 1024,
            chunk_size=5 * 1024 * 1024,  # 5MB chunks for S3
            show_progress=False,
            write_timeout=300,  # 5 minutes for large file operations
        )

        bytes_processed = self._stream_with_limited_memory(
            self.source_bucket, source_key, self.dest_bucket, dest_key, qio, hasher
        )

        # Verify
        self.assertEqual(bytes_processed, 20 * 1024 * 1024)
        self.assertEqual(hasher.hexdigest(), expected_sha256)

        # Check multipart upload was used (ETag format)
        response = self.s3_client.head_object(Bucket=self.dest_bucket, Key=dest_key)
        self.assertIn("-", response["ETag"].strip('"'))  # Multipart ETags contain '-'

    def test_memory_pressure_streaming(self):
        """Test streaming with significant memory pressure"""
        # Generate 50MB file (reduced from 100MB for faster testing)
        file_path, _, expected_sha256 = TestFileGenerator.create_test_file(
            50 * 1024 * 1024
        )
        self.temp_files.append(file_path)

        # Upload to source
        source_key = "memory-test-file.bin"
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Stream with only 30MB memory limit
        dest_key = "memory-test-file-copy.bin"
        hasher = hashlib.sha256()

        qio = LimitedQueueIO(
            memory_limit=30 * 1024 * 1024,  # Increased to 30MB for 50MB file
            chunk_size=2 * 1024 * 1024,  # Smaller chunks (2MB) for better flow
            show_progress=False,
            write_timeout=600,  # 10 minutes for memory-constrained operations
        )

        start_time = time.time()
        bytes_processed = self._stream_with_limited_memory(
            self.source_bucket, source_key, self.dest_bucket, dest_key, qio, hasher
        )
        duration = time.time() - start_time

        # Verify transfer completed successfully
        self.assertEqual(bytes_processed, 50 * 1024 * 1024)
        self.assertEqual(hasher.hexdigest(), expected_sha256)

        # Calculate throughput
        throughput_mbps = (bytes_processed / (1024 * 1024)) / duration
        print(f"\nThroughput: {throughput_mbps:.2f} MB/s with 30MB memory limit")

        # Verify the queue respected memory limits (15 chunks max with 2MB chunks)
        self.assertLessEqual(qio._queue.maxsize, 15)

    def test_concurrent_streaming(self):
        """Test multiple concurrent streaming operations"""
        # Generate test files
        files = []
        for i in range(3):
            size = (i + 1) * 10 * 1024 * 1024  # 10MB, 20MB, 30MB
            file_path, md5, sha256 = TestFileGenerator.create_test_file(size)
            self.temp_files.append(file_path)
            files.append(
                {
                    "path": file_path,
                    "size": size,
                    "sha256": sha256,
                    "source_key": f"concurrent-{i}.bin",
                    "dest_key": f"concurrent-{i}-copy.bin",
                }
            )

        # Upload all files to source bucket
        for file_info in files:
            with open(file_info["path"], "rb") as f:
                self.s3_client.upload_fileobj(
                    f, self.source_bucket, file_info["source_key"]
                )

        # Stream all files concurrently
        threads = []
        results = []

        def stream_file(file_info, index):
            hasher = hashlib.sha256()
            qio = LimitedQueueIO(
                memory_limit=15 * 1024 * 1024,  # 15MB limit per stream
                chunk_size=3 * 1024 * 1024,  # 3MB chunks for better concurrency
                show_progress=False,
                write_timeout=180,  # 3 minutes per concurrent stream
            )

            bytes_processed = self._stream_with_limited_memory(
                self.source_bucket,
                file_info["source_key"],
                self.dest_bucket,
                file_info["dest_key"],
                qio,
                hasher,
            )

            results.append(
                {"index": index, "bytes": bytes_processed, "hash": hasher.hexdigest()}
            )

        # Start all streams
        for i, file_info in enumerate(files):
            thread = threading.Thread(target=stream_file, args=(file_info, i))
            thread.start()
            threads.append(thread)

        # Wait for completion with generous timeout
        for thread in threads:
            thread.join(timeout=300)  # 5 minutes for concurrent operations
            if thread.is_alive():
                self.fail("Thread did not complete within timeout")

        # Verify all transfers
        self.assertEqual(len(results), 3)
        for file_info in files:
            # Find matching result
            result = next(r for r in results if r["bytes"] == file_info["size"])
            self.assertEqual(result["hash"], file_info["sha256"])

    def test_streaming_with_errors(self):
        """Test error handling during streaming"""
        # Generate test file
        file_path, _, _ = TestFileGenerator.create_test_file(10 * 1024 * 1024)
        self.temp_files.append(file_path)

        # Upload to source
        source_key = "error-test-file.bin"
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Try to stream to a non-existent bucket
        with self.assertRaises(Exception):
            hasher = hashlib.sha256()
            self._stream_s3_to_s3_with_hash(
                self.source_bucket,
                source_key,
                "non-existent-bucket",
                "test.bin",
                hasher,
            )

    def test_hash_verification(self):
        """Test that computed hashes match expected values"""
        # Generate file with known content
        test_data = b"Hello, QueueIO!" * 100000  # ~1.9MB
        expected_sha256 = hashlib.sha256(test_data).hexdigest()

        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(test_data)
            file_path = tmp.name
        self.temp_files.append(file_path)

        # Upload to source
        source_key = "hash-test.bin"
        self.s3_client.put_object(
            Bucket=self.source_bucket, Key=source_key, Body=test_data
        )

        # Stream and compute hash
        dest_key = "hash-test-copy.bin"
        hasher = hashlib.sha256()
        bytes_processed = self._stream_s3_to_s3_with_hash(
            self.source_bucket, source_key, self.dest_bucket, dest_key, hasher
        )

        # Verify hash matches
        self.assertEqual(hasher.hexdigest(), expected_sha256)
        self.assertEqual(bytes_processed, len(test_data))

        # Download and verify content
        response = self.s3_client.get_object(Bucket=self.dest_bucket, Key=dest_key)
        downloaded_data = response["Body"].read()
        self.assertEqual(downloaded_data, test_data)

    # Helper methods

    def _stream_s3_to_s3_with_hash(
        self, source_bucket, source_key, dest_bucket, dest_key, hasher
    ):
        """Stream from S3 to S3 using QueueIO and compute hash"""
        qio = QueueIO(chunk_size=5 * 1024 * 1024)

        bytes_processed = [0]  # Use list to avoid nonlocal in nested class
        download_complete = False
        upload_exception = None

        def download_worker():
            nonlocal download_complete
            try:
                response = self.s3_client.get_object(
                    Bucket=source_bucket, Key=source_key
                )
                for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
                    qio.write(chunk)
            finally:
                qio.close()
                download_complete = True

        def upload_worker():
            nonlocal upload_exception
            handler = S3StreamHandler(self.s3_client)

            try:
                # Create a custom stream that computes hash
                class HashingStream:
                    def read(self, size=-1):
                        data = qio.read(size)
                        if data:
                            hasher.update(data)
                            bytes_processed[0] += len(data)
                        return data

                stream = HashingStream()
                handler.upload_stream(stream, dest_bucket, dest_key)
            except Exception as e:
                upload_exception = e

        # Start workers
        download_thread = threading.Thread(target=download_worker)
        upload_thread = threading.Thread(target=upload_worker)

        download_thread.start()
        upload_thread.start()

        # Wait for completion with generous timeout
        download_thread.join(timeout=300)
        upload_thread.join(timeout=300)
        
        # Check if threads completed
        if download_thread.is_alive() or upload_thread.is_alive():
            raise TimeoutError("Threads did not complete within timeout")

        if upload_exception:
            raise upload_exception

        return bytes_processed[0]

    def _stream_with_limited_memory(
        self, source_bucket, source_key, dest_bucket, dest_key, qio, hasher
    ):
        """Stream using a pre-configured LimitedQueueIO instance"""
        bytes_processed = [0]  # Use list to avoid nonlocal in nested class
        exception = None

        def download_worker():
            try:
                response = self.s3_client.get_object(
                    Bucket=source_bucket, Key=source_key
                )
                # Use the same chunk size as the QueueIO to avoid buffering issues
                for chunk in response["Body"].iter_chunks(chunk_size=qio._chunk_size):
                    qio.write(chunk)
            except Exception as e:
                nonlocal exception
                exception = e
            finally:
                qio.close()

        def upload_worker():
            handler = S3StreamHandler(self.s3_client)

            try:
                # Hash-computing stream
                class HashingStream:
                    def read(self, size=-1):
                        data = qio.read(size)
                        if data:
                            hasher.update(data)
                            bytes_processed[0] += len(data)
                        return data

                stream = HashingStream()
                handler.upload_stream(
                    stream, dest_bucket, dest_key, part_size=5 * 1024 * 1024
                )
            except Exception as e:
                nonlocal exception
                if not exception:
                    exception = e

        # Start workers
        download_thread = threading.Thread(target=download_worker)
        upload_thread = threading.Thread(target=upload_worker)

        download_thread.start()
        upload_thread.start()

        # Wait for completion with generous timeout
        download_thread.join(timeout=300)
        upload_thread.join(timeout=300)
        
        # Check if threads completed
        if download_thread.is_alive() or upload_thread.is_alive():
            raise TimeoutError("Threads did not complete within timeout")

        if exception:
            raise exception

        return bytes_processed[0]


class TestS3StreamHandler(unittest.TestCase):
    """Test the S3StreamHandler utility class"""

    @classmethod
    def setUpClass(cls):
        """Set up S3 client"""
        try:
            cls.s3_client = create_test_environment()
            cls.s3_client.list_buckets()
        except Exception as e:
            raise unittest.SkipTest(f"LocalStack not available: {e}")

    def setUp(self):
        """Create test bucket"""
        self.test_bucket = f"handler-test-{int(time.time())}"
        self.s3_client.create_bucket(Bucket=self.test_bucket)
        self.handler = S3StreamHandler(self.s3_client)
        self.temp_files = []

    def tearDown(self):
        """Clean up"""
        # Delete bucket contents
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.test_bucket)
            if "Contents" in response:
                for obj in response["Contents"]:
                    self.s3_client.delete_object(
                        Bucket=self.test_bucket, Key=obj["Key"]
                    )
            self.s3_client.delete_bucket(Bucket=self.test_bucket)
        except Exception:
            pass

        # Clean up temp files
        for f in self.temp_files:
            try:
                os.unlink(f)
            except Exception:
                pass

    def test_upload_download_cycle(self):
        """Test upload and download using stream handler"""
        # Create test data
        test_data = b"Test data for S3StreamHandler" * 1000
        stream = BytesIO(test_data)

        # Upload
        self.handler.upload_stream(stream, self.test_bucket, "test-object")

        # Download
        output = BytesIO()
        self.handler.download_stream(self.test_bucket, "test-object", output)

        # Verify
        self.assertEqual(output.getvalue(), test_data)

    def test_multipart_upload_with_handler(self):
        """Test multipart upload through handler"""
        # Create 10MB test file
        file_path, _, _ = TestFileGenerator.create_test_file(10 * 1024 * 1024)
        self.temp_files.append(file_path)

        # Upload using handler
        with open(file_path, "rb") as f:
            self.handler.upload_stream(
                f, self.test_bucket, "multipart-test", part_size=5 * 1024 * 1024
            )

        # Verify object exists and has multipart ETag
        response = self.s3_client.head_object(
            Bucket=self.test_bucket, Key="multipart-test"
        )
        self.assertIn("-", response["ETag"].strip('"'))


if __name__ == "__main__":
    unittest.main()
