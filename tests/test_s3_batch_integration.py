#!/usr/bin/env python3
"""
Comprehensive S3 Batch Integration Tests for QueueIO using LocalStack

This module tests copying multiple files between S3 buckets while computing
hashes during transfer and verifying file integrity.
"""

import unittest
import os
import sys
import time
import threading
import hashlib
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import QueueIO, LimitedQueueIO

try:
    # Try relative import first (when run as package)
    from .s3_test_utils import (
        S3StreamHandler,
        TestFileGenerator,
        create_test_environment,
    )
except ImportError:
    # Fall back to absolute import (when run directly or by VSCode)
    from s3_test_utils import (
        S3StreamHandler,
        TestFileGenerator,
        create_test_environment,
    )


class TestS3BatchOperations(unittest.TestCase):
    """Test batch S3 operations with QueueIO"""

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
        self.source_bucket = f"test-batch-source-{int(time.time())}"
        self.dest_bucket = f"test-batch-dest-{int(time.time())}"

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

    def test_batch_file_copy_with_hash_verification(self):
        """Test copying multiple files between buckets with hash verification"""
        print("\n=== Testing Batch File Copy with Hash Verification ===")

        # Generate test files of various sizes
        test_files = [
            {"name": "small-1.bin", "size": 1 * 1024 * 1024},  # 1MB
            {"name": "small-2.bin", "size": 2 * 1024 * 1024},  # 2MB
            {"name": "medium-1.bin", "size": 10 * 1024 * 1024},  # 10MB
            {"name": "medium-2.bin", "size": 15 * 1024 * 1024},  # 15MB
            {"name": "large-1.bin", "size": 25 * 1024 * 1024},  # 25MB
        ]

        # Generate and upload test files
        file_metadata = {}
        print("\nGenerating and uploading test files...")

        for file_info in test_files:
            # Generate test file
            file_path, md5_hash, sha256_hash = TestFileGenerator.create_test_file(
                file_info["size"]
            )
            self.temp_files.append(file_path)

            # Store metadata
            file_metadata[file_info["name"]] = {
                "size": file_info["size"],
                "md5": md5_hash,
                "sha256": sha256_hash,
                "source_key": file_info["name"],
                "dest_key": f"copied-{file_info['name']}",
            }

            # Upload to source bucket
            print(
                f"  Uploading {file_info['name']} ({file_info['size'] / 1024 / 1024:.1f}MB)"
            )
            with open(file_path, "rb") as f:
                self.s3_client.upload_fileobj(f, self.source_bucket, file_info["name"])

        # Copy files with hash computation
        print("\nCopying files with hash computation...")
        computed_hashes = {}
        copy_results = []

        def copy_with_hash(file_name, metadata):
            """Copy a single file and compute its hash"""
            start_time = time.time()
            sha256_hasher = hashlib.sha256()
            md5_hasher = hashlib.md5()

            # Use LimitedQueueIO to manage memory
            qio = LimitedQueueIO(
                memory_limit=20 * 1024 * 1024,  # 20MB memory limit
                chunk_size=5 * 1024 * 1024,  # 5MB chunks
                show_progress=False,
                write_timeout=None,
            )

            bytes_transferred = 0
            exception = None

            def download_worker():
                nonlocal exception
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.source_bucket, Key=metadata["source_key"]
                    )
                    for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
                        qio.write(chunk)
                except Exception as e:
                    exception = e
                finally:
                    qio.close()

            def upload_worker():
                nonlocal bytes_transferred, exception
                handler = S3StreamHandler(self.s3_client)

                try:
                    # Create a hashing stream
                    class HashingStream:
                        def read(self, size=-1):
                            data = qio.read(size)
                            if data:
                                sha256_hasher.update(data)
                                md5_hasher.update(data)
                                nonlocal bytes_transferred
                                bytes_transferred += len(data)
                            return data

                    stream = HashingStream()
                    handler.upload_stream(
                        stream,
                        self.dest_bucket,
                        metadata["dest_key"],
                        part_size=5 * 1024 * 1024,
                    )
                except Exception as e:
                    if not exception:
                        exception = e

            # Start transfer
            download_thread = threading.Thread(target=download_worker)
            upload_thread = threading.Thread(target=upload_worker)

            download_thread.start()
            upload_thread.start()

            download_thread.join(timeout=120)
            upload_thread.join(timeout=120)

            if exception:
                raise exception

            duration = time.time() - start_time
            throughput_mbps = (bytes_transferred / (1024 * 1024)) / duration

            return {
                "file": file_name,
                "bytes_transferred": bytes_transferred,
                "duration": duration,
                "throughput_mbps": throughput_mbps,
                "computed_sha256": sha256_hasher.hexdigest(),
                "computed_md5": md5_hasher.hexdigest(),
            }

        # Copy files concurrently
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_file = {
                executor.submit(copy_with_hash, file_name, metadata): file_name
                for file_name, metadata in file_metadata.items()
            }

            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    result = future.result()
                    copy_results.append(result)
                    computed_hashes[file_name] = {
                        "sha256": result["computed_sha256"],
                        "md5": result["computed_md5"],
                    }
                    print(f"  ✓ {file_name}: {result['throughput_mbps']:.2f} MB/s")
                except Exception as e:
                    self.fail(f"Failed to copy {file_name}: {e}")

        # Verify all files were copied correctly
        print("\nVerifying copied files...")

        # List all objects in destination bucket
        response = self.s3_client.list_objects_v2(Bucket=self.dest_bucket)
        self.assertIn("Contents", response)
        copied_objects = {obj["Key"]: obj for obj in response["Contents"]}

        # Verify each file
        for file_name, metadata in file_metadata.items():
            dest_key = metadata["dest_key"]

            # Check file exists
            self.assertIn(dest_key, copied_objects)

            # Check file size
            self.assertEqual(
                copied_objects[dest_key]["Size"],
                metadata["size"],
                f"Size mismatch for {dest_key}",
            )

            # Verify computed hashes match expected
            self.assertEqual(
                computed_hashes[file_name]["sha256"],
                metadata["sha256"],
                f"SHA256 mismatch for {file_name}",
            )
            self.assertEqual(
                computed_hashes[file_name]["md5"],
                metadata["md5"],
                f"MD5 mismatch for {file_name}",
            )

            print(f"  ✓ {file_name}: Size and hashes match")

        # Download and verify content of one file
        print("\nVerifying content integrity...")
        test_file = "medium-1.bin"
        metadata = file_metadata[test_file]

        response = self.s3_client.get_object(
            Bucket=self.dest_bucket, Key=metadata["dest_key"]
        )
        downloaded_data = response["Body"].read()

        # Compute hash of downloaded data
        downloaded_sha256 = hashlib.sha256(downloaded_data).hexdigest()
        self.assertEqual(
            downloaded_sha256, metadata["sha256"], "Downloaded file hash doesn't match"
        )
        print(f"  ✓ Content verification passed for {test_file}")

        # Print summary
        print("\n=== Summary ===")
        print(f"Files copied: {len(copy_results)}")
        total_bytes = sum(r["bytes_transferred"] for r in copy_results)
        total_time = sum(r["duration"] for r in copy_results)
        avg_throughput = (total_bytes / (1024 * 1024)) / total_time
        print(f"Total data transferred: {total_bytes / 1024 / 1024:.1f} MB")
        print(f"Average throughput: {avg_throughput:.2f} MB/s")
        print("All hashes verified successfully!")

    def test_large_file_streaming_with_progress(self):
        """Test streaming a large file with progress tracking"""
        print("\n=== Testing Large File Streaming with Progress ===")

        # Generate a 100MB test file
        file_size = 100 * 1024 * 1024
        file_path, expected_md5, expected_sha256 = TestFileGenerator.create_test_file(
            file_size
        )
        self.temp_files.append(file_path)

        # Upload to source bucket
        source_key = "large-file-100mb.bin"
        print(f"Uploading {file_size / 1024 / 1024}MB test file...")
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Stream with progress tracking
        dest_key = "large-file-100mb-copy.bin"
        print("Streaming with hash computation and progress tracking...")

        # Use LimitedQueueIO with progress bar
        qio = LimitedQueueIO(
            memory_limit=50 * 1024 * 1024,  # 50MB memory limit for 100MB file
            chunk_size=10 * 1024 * 1024,  # 10MB chunks
            show_progress=True,  # Enable progress bar
            write_timeout=None,
        )

        sha256_hasher = hashlib.sha256()
        bytes_processed = 0
        exception = None

        def download_worker():
            nonlocal exception
            try:
                response = self.s3_client.get_object(
                    Bucket=self.source_bucket, Key=source_key
                )
                # Download in chunks
                for chunk in response["Body"].iter_chunks(chunk_size=5 * 1024 * 1024):
                    qio.write(chunk)
            except Exception as e:
                exception = e
            finally:
                qio.close()

        def upload_worker():
            nonlocal bytes_processed, exception
            handler = S3StreamHandler(self.s3_client)

            try:
                # Stream with hash computation
                class HashingStream:
                    def read(self, size=-1):
                        data = qio.read(size)
                        if data:
                            sha256_hasher.update(data)
                            nonlocal bytes_processed
                            bytes_processed += len(data)
                        return data

                stream = HashingStream()
                handler.upload_stream(
                    stream, self.dest_bucket, dest_key, part_size=10 * 1024 * 1024
                )
            except Exception as e:
                if not exception:
                    exception = e

        # Measure transfer time
        start_time = time.time()

        download_thread = threading.Thread(target=download_worker)
        upload_thread = threading.Thread(target=upload_worker)

        download_thread.start()
        upload_thread.start()

        download_thread.join(timeout=300)
        upload_thread.join(timeout=300)

        duration = time.time() - start_time

        if exception:
            raise exception

        # Verify results
        self.assertEqual(bytes_processed, file_size)
        self.assertEqual(sha256_hasher.hexdigest(), expected_sha256)

        throughput_mbps = (bytes_processed / (1024 * 1024)) / duration
        print(f"\nTransfer completed:")
        print(f"  Time: {duration:.2f}s")
        print(f"  Throughput: {throughput_mbps:.2f} MB/s")
        print(f"  SHA256: {sha256_hasher.hexdigest()[:16]}... (verified)")

        # Verify the copy
        response = self.s3_client.head_object(Bucket=self.dest_bucket, Key=dest_key)
        self.assertEqual(response["ContentLength"], file_size)

        # Check if multipart upload was used (ETag contains dash)
        self.assertIn("-", response["ETag"].strip('"'))
        print(f"  Multipart upload: Yes")


if __name__ == "__main__":
    unittest.main()
