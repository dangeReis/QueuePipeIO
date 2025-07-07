#!/usr/bin/env python3
"""
Tests for HashingLimitedQueueIO - read-side hash computation

This module tests the HashingLimitedQueueIO class which computes
hashes during read operations (consumer-side hashing).
"""

import unittest
import threading
import hashlib
import time
import os
import sys

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path setup
from queuepipeio import HashingLimitedQueueIO, LimitedQueueIO  # noqa: E402


class TestHashingLimitedQueueIO(unittest.TestCase):
    """Test HashingLimitedQueueIO functionality"""

    def test_basic_hash_computation(self):
        """Test basic hash computation during read"""
        test_data = b"Hello, World! This is a test of HashingLimitedQueueIO."
        expected_hash = hashlib.sha256(test_data).hexdigest()

        # Create queue
        queue = HashingLimitedQueueIO()

        # Write data
        queue.write(test_data)
        queue.close()

        # Read data and compute hash
        read_data = queue.read()

        # Verify
        self.assertEqual(read_data, test_data)
        self.assertEqual(queue.get_hash(), expected_hash)
        self.assertEqual(queue.get_bytes_hashed(), len(test_data))

    def test_multiple_hash_algorithms(self):
        """Test different hash algorithms"""
        test_data = b"Test data for multiple hash algorithms" * 100

        algorithms = ["md5", "sha1", "sha256", "sha512", "sha3_256"]
        for algo in algorithms:
            with self.subTest(algorithm=algo):
                # Skip if algorithm not available
                try:
                    hashlib.new(algo)
                except ValueError:
                    self.skipTest(f"Algorithm {algo} not available")

                # Create queue with specific algorithm
                queue = HashingLimitedQueueIO(hash_algorithm=algo)

                # Write and close
                queue.write(test_data)
                queue.close()

                # Read all data
                read_data = queue.read()

                # Verify hash
                expected = hashlib.new(algo, test_data).hexdigest()
                self.assertEqual(queue.get_hash(), expected)
                self.assertEqual(read_data, test_data)

    def test_partial_reads(self):
        """Test hash computation with partial reads"""
        test_data = b"0123456789" * 1000  # 10KB
        expected_hash = hashlib.sha256(test_data).hexdigest()

        queue = HashingLimitedQueueIO()
        queue.write(test_data)
        queue.close()

        # Read in small chunks
        chunks = []
        while True:
            chunk = queue.read(100)  # Read 100 bytes at a time
            if not chunk:
                break
            chunks.append(chunk)

        # Verify
        self.assertEqual(b"".join(chunks), test_data)
        self.assertEqual(queue.get_hash(), expected_hash)
        self.assertEqual(queue.get_bytes_hashed(), len(test_data))

    def test_memory_limited_transfer(self):
        """Test with memory limits"""
        # 5MB of data with 1MB memory limit
        data_size = 5 * 1024 * 1024
        test_data = os.urandom(data_size)
        expected_hash = hashlib.sha256(test_data).hexdigest()

        # Create memory-limited queue
        queue = HashingLimitedQueueIO(
            memory_limit=1 * 1024 * 1024,  # 1MB limit
            chunk_size=256 * 1024,  # 256KB chunks
        )

        # Producer thread
        def producer():
            # Write in chunks
            chunk_size = 128 * 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i : i + chunk_size]
                queue.write(chunk)
            queue.close()

        # Start producer
        producer_thread = threading.Thread(target=producer)
        producer_thread.start()

        # Consumer reads and hash is computed
        read_data = []
        while True:
            chunk = queue.read(64 * 1024)  # Read 64KB at a time
            if not chunk:
                break
            read_data.append(chunk)

        producer_thread.join()

        # Verify
        self.assertEqual(b"".join(read_data), test_data)
        self.assertEqual(queue.get_hash(), expected_hash)
        self.assertEqual(queue.get_bytes_hashed(), data_size)

    def test_concurrent_read_write(self):
        """Test concurrent reading and writing"""
        # Generate test data
        data_size = 10 * 1024 * 1024  # 10MB
        test_data = os.urandom(data_size)
        expected_hash = hashlib.sha256(test_data).hexdigest()

        queue = HashingLimitedQueueIO(
            memory_limit=2 * 1024 * 1024,  # 2MB limit
            chunk_size=512 * 1024,  # 512KB chunks
        )

        write_complete = False
        read_data = []

        def writer():
            nonlocal write_complete
            # Write in chunks
            chunk_size = 256 * 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i : i + chunk_size]
                queue.write(chunk)
                time.sleep(0.0001)  # Simulate some work
            queue.close()
            write_complete = True

        def reader():
            # Read continuously
            while True:
                chunk = queue.read(128 * 1024)
                if not chunk:
                    if write_complete:
                        break
                    time.sleep(0.0001)
                else:
                    read_data.append(chunk)

        # Start threads
        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)

        start_time = time.time()
        writer_thread.start()
        reader_thread.start()

        writer_thread.join()
        reader_thread.join()
        duration = time.time() - start_time

        # Verify
        self.assertEqual(b"".join(read_data), test_data)
        self.assertEqual(queue.get_hash(), expected_hash)
        self.assertEqual(queue.get_bytes_hashed(), data_size)

        throughput = (data_size / (1024 * 1024)) / duration
        print(f"\nConcurrent throughput: {throughput:.2f} MB/s")

    def test_reset_hash(self):
        """Test resetting hash computation"""
        data1 = b"First data set"
        data2 = b"Second data set"

        queue = HashingLimitedQueueIO()

        # First operation
        queue.write(data1)
        queue.close()
        _ = queue.read()
        hash1 = queue.get_hash()

        # Reset and compute new hash
        queue.reset_hash()

        # Need to create new queue for second operation
        queue2 = HashingLimitedQueueIO()
        queue2.write(data2)
        queue2.close()
        _ = queue2.read()
        hash2 = queue2.get_hash()

        # Verify hashes are different
        self.assertNotEqual(hash1, hash2)
        self.assertEqual(hash1, hashlib.sha256(data1).hexdigest())
        self.assertEqual(hash2, hashlib.sha256(data2).hexdigest())

    def test_empty_read(self):
        """Test behavior with empty reads"""
        queue = HashingLimitedQueueIO()
        queue.close()

        # Read from empty queue
        data = queue.read()
        self.assertEqual(data, b"")
        self.assertEqual(queue.get_bytes_hashed(), 0)

        # Hash of empty data
        expected_empty_hash = hashlib.sha256(b"").hexdigest()
        self.assertEqual(queue.get_hash(), expected_empty_hash)

    def test_progress_with_hashing(self):
        """Test progress bar with hashing"""
        data_size = 5 * 1024 * 1024  # 5MB
        test_data = os.urandom(data_size)

        # Create queue with progress bar
        queue = HashingLimitedQueueIO(
            memory_limit=2 * 1024 * 1024,
            chunk_size=512 * 1024,
            show_progress=True,
        )

        # Write data
        chunk_size = 256 * 1024
        for i in range(0, len(test_data), chunk_size):
            chunk = test_data[i : i + chunk_size]
            queue.write(chunk)
        queue.close()

        # Read data
        read_chunks = []
        while True:
            chunk = queue.read(128 * 1024)
            if not chunk:
                break
            read_chunks.append(chunk)

        # Verify
        self.assertEqual(b"".join(read_chunks), test_data)
        self.assertEqual(queue.get_bytes_hashed(), data_size)

    def test_comparison_with_write_side_hashing(self):
        """Compare read-side vs write-side hashing approaches"""
        test_data = os.urandom(1024 * 1024)  # 1MB

        # Read-side hashing (HashingLimitedQueueIO)
        read_side_queue = HashingLimitedQueueIO()
        read_side_queue.write(test_data)
        read_side_queue.close()
        _ = read_side_queue.read()
        read_side_hash = read_side_queue.get_hash()

        # Write-side hashing (manual)
        write_side_queue = LimitedQueueIO()
        hasher = hashlib.sha256()
        hasher.update(test_data)
        write_side_queue.write(test_data)
        write_side_queue.close()
        _ = write_side_queue.read()
        write_side_hash = hasher.hexdigest()

        # Both should produce same hash
        self.assertEqual(read_side_hash, write_side_hash)
        self.assertEqual(read_side_hash, hashlib.sha256(test_data).hexdigest())

    def test_multiple_consumers_different_hashes(self):
        """Test multiple consumers computing different hashes"""
        test_data = b"Shared data for multiple consumers" * 1000

        # Note: In real use, you'd have separate reader instances
        # This test demonstrates the concept

        # Consumer 1: SHA256
        queue1 = HashingLimitedQueueIO(hash_algorithm="sha256")
        queue1.write(test_data)
        queue1.close()
        _ = queue1.read()
        sha256_hash = queue1.get_hash()

        # Consumer 2: MD5
        queue2 = HashingLimitedQueueIO(hash_algorithm="md5")
        queue2.write(test_data)
        queue2.close()
        _ = queue2.read()
        md5_hash = queue2.get_hash()

        # Verify different algorithms produce different hashes
        self.assertNotEqual(sha256_hash, md5_hash)
        self.assertEqual(sha256_hash, hashlib.sha256(test_data).hexdigest())
        self.assertEqual(md5_hash, hashlib.md5(test_data).hexdigest())


class TestHashingLimitedQueueIOIntegration(unittest.TestCase):
    """Integration tests for HashingLimitedQueueIO"""

    def test_simulated_s3_download_verification(self):
        """Simulate S3 download with hash verification"""
        # Simulate a file with known hash
        file_size = 25 * 1024 * 1024  # 25MB
        file_data = os.urandom(file_size)
        expected_hash = hashlib.sha256(file_data).hexdigest()

        # Simulate S3 providing hash in metadata
        s3_metadata = {"sha256": expected_hash}

        # Create hashing queue
        queue = HashingLimitedQueueIO(
            hash_algorithm="sha256",
            memory_limit=10 * 1024 * 1024,  # 10MB limit
            chunk_size=2 * 1024 * 1024,  # 2MB chunks
        )

        download_complete = False
        verification_passed = False

        def s3_download():
            """Simulate S3 download writing to queue"""
            nonlocal download_complete
            # Simulate downloading in chunks
            chunk_size = 1024 * 1024  # 1MB chunks
            for i in range(0, len(file_data), chunk_size):
                chunk = file_data[i : i + chunk_size]
                queue.write(chunk)
                time.sleep(0.0001)  # Simulate network delay
            queue.close()
            download_complete = True

        def local_storage():
            """Simulate writing to local storage while verifying"""
            nonlocal verification_passed
            received_data = []

            while True:
                chunk = queue.read(512 * 1024)  # Read 512KB at a time
                if not chunk:
                    if download_complete:
                        break
                    time.sleep(0.0001)
                else:
                    received_data.append(chunk)

            # Verify hash after reading all data
            computed_hash = queue.get_hash()
            verification_passed = computed_hash == s3_metadata["sha256"]

            # Also verify data integrity
            self.assertEqual(b"".join(received_data), file_data)

        # Run simulation
        download_thread = threading.Thread(target=s3_download)
        storage_thread = threading.Thread(target=local_storage)

        start_time = time.time()
        download_thread.start()
        storage_thread.start()

        download_thread.join()
        storage_thread.join()
        duration = time.time() - start_time

        # Verify
        self.assertTrue(verification_passed)
        self.assertEqual(queue.get_bytes_hashed(), file_size)

        throughput = (file_size / (1024 * 1024)) / duration
        print(f"\nS3 simulation throughput: {throughput:.2f} MB/s")
        print(f"Hash verification: {'PASSED' if verification_passed else 'FAILED'}")


if __name__ == "__main__":
    unittest.main()
