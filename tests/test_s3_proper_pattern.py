#!/usr/bin/env python3
"""
S3 Integration Tests using the PROPER QueueIO pattern

This demonstrates the correct way to use QueueIO for S3 operations:
1. S3 download writes to HashingQueueIO
2. HashingQueueIO computes hash as data flows through
3. S3 upload reads from HashingQueueIO
4. Hash is available after transfer completes
"""

import unittest
import os
import sys
import time
import threading
import hashlib

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


class HashingQueueIO(QueueIO):
    """QueueIO that computes hash of data passing through"""

    def __init__(self, hash_algorithm="sha256", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hasher = hashlib.new(hash_algorithm)
        self._hash_lock = threading.Lock()
        self._bytes_hashed = 0

    def write(self, b):
        """Write data and update hash"""
        with self._hash_lock:
            self._hasher.update(b)
            self._bytes_hashed += len(b)
        return super().write(b)

    def get_hash(self):
        """Get the computed hash (hexdigest)"""
        with self._hash_lock:
            return self._hasher.hexdigest()

    def get_bytes_hashed(self):
        """Get the number of bytes hashed"""
        with self._hash_lock:
            return self._bytes_hashed


class LimitedHashingQueueIO(LimitedQueueIO):
    """Memory-limited QueueIO with hash computation"""

    def __init__(self, hash_algorithm="sha256", **kwargs):
        super().__init__(**kwargs)
        self._hasher = hashlib.new(hash_algorithm)
        self._hash_lock = threading.Lock()
        self._bytes_hashed = 0

    def write(self, b):
        """Write data and update hash"""
        with self._hash_lock:
            self._hasher.update(b)
            self._bytes_hashed += len(b)
        return super().write(b)

    def get_hash(self):
        """Get the computed hash (hexdigest)"""
        with self._hash_lock:
            return self._hasher.hexdigest()

    def get_bytes_hashed(self):
        """Get the number of bytes hashed"""
        with self._hash_lock:
            return self._bytes_hashed


class TestS3ProperPattern(unittest.TestCase):
    """Test S3 operations using the proper QueueIO pattern"""

    @classmethod
    def setUpClass(cls):
        """Set up S3 client for all tests"""
        try:
            cls.s3_client = create_test_environment()
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
        for bucket in [self.source_bucket, self.dest_bucket]:
            try:
                response = self.s3_client.list_objects_v2(Bucket=bucket)
                if "Contents" in response:
                    for obj in response["Contents"]:
                        self.s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                self.s3_client.delete_bucket(Bucket=bucket)
            except Exception:
                pass

        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
            except Exception:
                pass

    def test_proper_s3_streaming_pattern(self):
        """Test the PROPER way to stream S3 objects with hash computation"""
        # Generate a 25MB test file
        file_size = 25 * 1024 * 1024
        file_path, _, expected_sha256 = TestFileGenerator.create_test_file(file_size)
        self.temp_files.append(file_path)

        # Upload to source bucket
        source_key = "proper-pattern-test.bin"
        print(f"\nUploading {file_size / (1024*1024):.1f}MB test file...")
        with open(file_path, "rb") as f:
            self.s3_client.upload_fileobj(f, self.source_bucket, source_key)

        # Create HashingQueueIO with memory limit
        hashing_queue = LimitedHashingQueueIO(
            hash_algorithm="sha256",
            memory_limit=10 * 1024 * 1024,  # 10MB limit for 25MB file
            chunk_size=2 * 1024 * 1024,  # 2MB chunks
            show_progress=False,
        )

        dest_key = "proper-pattern-copy.bin"
        exception = None
        bytes_transferred = 0

        def s3_download_to_queue():
            """S3 download writes to HashingQueueIO"""
            nonlocal exception
            try:
                response = self.s3_client.get_object(
                    Bucket=self.source_bucket, Key=source_key
                )
                # S3 downloads and writes to our hashing queue
                for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
                    hashing_queue.write(chunk)
                print(
                    f"Download complete, wrote {hashing_queue.get_bytes_hashed() / (1024*1024):.1f}MB"
                )
            except Exception as e:
                exception = e
            finally:
                hashing_queue.close()  # Signal EOF

        def s3_upload_from_queue():
            """S3 upload reads from HashingQueueIO"""
            nonlocal exception, bytes_transferred
            try:
                handler = S3StreamHandler(self.s3_client)

                # S3 uploads by reading from our hashing queue
                # The queue is already computing the hash!
                handler.upload_stream(
                    hashing_queue,  # Direct stream from queue
                    self.dest_bucket,
                    dest_key,
                    part_size=5 * 1024 * 1024,
                )

                bytes_transferred = hashing_queue.get_bytes_hashed()
                print(f"Upload complete, read {bytes_transferred / (1024*1024):.1f}MB")
            except Exception as e:
                if not exception:
                    exception = e

        # Start both operations
        start_time = time.time()

        download_thread = threading.Thread(target=s3_download_to_queue)
        upload_thread = threading.Thread(target=s3_upload_from_queue)

        download_thread.start()
        upload_thread.start()

        download_thread.join()
        upload_thread.join()

        duration = time.time() - start_time

        if exception:
            raise exception

        # Get the computed hash
        computed_hash = hashing_queue.get_hash()
        throughput_mbps = (bytes_transferred / (1024 * 1024)) / duration

        print(f"\nResults:")
        print(f"  Transfer time: {duration:.2f}s")
        print(f"  Throughput: {throughput_mbps:.2f} MB/s")
        print(f"  Computed hash: {computed_hash[:32]}...")
        print(f"  Expected hash: {expected_sha256[:32]}...")
        print(
            f"  Memory used: {hashing_queue._queue.maxsize * hashing_queue._chunk_size / (1024*1024):.1f}MB"
        )

        # Verify results
        self.assertEqual(bytes_transferred, file_size)
        self.assertEqual(computed_hash, expected_sha256)

        # Verify the destination file
        response = self.s3_client.head_object(Bucket=self.dest_bucket, Key=dest_key)
        self.assertEqual(response["ContentLength"], file_size)

    def test_batch_transfer_with_hashes(self):
        """Test transferring multiple files with hash computation"""
        files = [
            {"name": "file1.bin", "size": 5 * 1024 * 1024},  # 5MB
            {"name": "file2.bin", "size": 10 * 1024 * 1024},  # 10MB
            {"name": "file3.bin", "size": 15 * 1024 * 1024},  # 15MB
        ]

        # Generate and upload test files
        file_metadata = {}
        for file_info in files:
            file_path, _, sha256 = TestFileGenerator.create_test_file(file_info["size"])
            self.temp_files.append(file_path)

            file_metadata[file_info["name"]] = {
                "size": file_info["size"],
                "expected_hash": sha256,
                "computed_hash": None,
                "throughput": None,
            }

            with open(file_path, "rb") as f:
                self.s3_client.upload_fileobj(f, self.source_bucket, file_info["name"])

        # Transfer each file with hash computation
        for file_name, metadata in file_metadata.items():
            print(
                f"\nTransferring {file_name} ({metadata['size'] / (1024*1024):.1f}MB)..."
            )

            # Create a fresh hashing queue for each file
            hashing_queue = HashingQueueIO(chunk_size=2 * 1024 * 1024)

            exception = None

            def download():
                nonlocal exception
                try:
                    response = self.s3_client.get_object(
                        Bucket=self.source_bucket, Key=file_name
                    )
                    for chunk in response["Body"].iter_chunks(chunk_size=512 * 1024):
                        hashing_queue.write(chunk)
                except Exception as e:
                    exception = e
                finally:
                    hashing_queue.close()

            def upload():
                nonlocal exception
                try:
                    handler = S3StreamHandler(self.s3_client)
                    handler.upload_stream(
                        hashing_queue,
                        self.dest_bucket,
                        f"hashed-{file_name}",
                        part_size=5 * 1024 * 1024,
                    )
                except Exception as e:
                    if not exception:
                        exception = e

            start_time = time.time()

            dl_thread = threading.Thread(target=download)
            ul_thread = threading.Thread(target=upload)

            dl_thread.start()
            ul_thread.start()

            dl_thread.join()
            ul_thread.join()

            duration = time.time() - start_time

            if exception:
                raise exception

            # Store results
            metadata["computed_hash"] = hashing_queue.get_hash()
            metadata["throughput"] = (metadata["size"] / (1024 * 1024)) / duration

            print(f"  Hash: {metadata['computed_hash'][:16]}... ✓")
            print(f"  Throughput: {metadata['throughput']:.2f} MB/s")

        # Verify all hashes
        print("\nVerifying all hashes...")
        for file_name, metadata in file_metadata.items():
            self.assertEqual(
                metadata["computed_hash"],
                metadata["expected_hash"],
                f"Hash mismatch for {file_name}",
            )
            print(f"  {file_name}: Hash verified ✓")

        # Calculate total throughput
        total_size = sum(m["size"] for m in file_metadata.values())
        avg_throughput = sum(m["throughput"] for m in file_metadata.values()) / len(
            file_metadata
        )

        print(f"\nTotal data transferred: {total_size / (1024*1024):.1f}MB")
        print(f"Average throughput: {avg_throughput:.2f} MB/s")


if __name__ == "__main__":
    unittest.main()
