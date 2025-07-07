#!/usr/bin/env python3
"""
Test proper QueueIO usage pattern: streaming with hash computation

This demonstrates the correct pattern where:
1. A downloader writes to a HashingQueueIO
2. The HashingQueueIO computes hash as data flows through
3. An uploader reads from the HashingQueueIO
4. The hash is available after streaming completes
"""

import unittest
import threading
import hashlib
import time
import os
import sys
from io import BytesIO

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import QueueIO, LimitedQueueIO


class HashingQueueIO(QueueIO):
    """QueueIO that computes hash of data passing through"""

    def __init__(self, hash_algorithm="sha256", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hasher = hashlib.new(hash_algorithm)
        self._hash_lock = threading.Lock()
        self._bytes_hashed = 0

    def write(self, b):
        """Write data and update hash"""
        # Update hash with the data
        with self._hash_lock:
            self._hasher.update(b)
            self._bytes_hashed += len(b)

        # Pass through to parent
        return super().write(b)

    def get_hash(self):
        """Get the computed hash (hexdigest)"""
        with self._hash_lock:
            return self._hasher.hexdigest()

    def get_bytes_hashed(self):
        """Get the number of bytes hashed"""
        with self._hash_lock:
            return self._bytes_hashed


class TestHashStreaming(unittest.TestCase):
    """Test streaming with hash computation"""

    def test_simple_hash_streaming(self):
        """Test basic streaming with hash computation"""
        # Create test data
        test_data = b"Hello, World! " * 1000  # ~14KB
        expected_hash = hashlib.sha256(test_data).hexdigest()

        # Create hashing queue
        hashing_queue = HashingQueueIO(chunk_size=4096)

        # Simulate downloader writing to queue
        def downloader():
            # Write in chunks like a real download
            chunk_size = 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i : i + chunk_size]
                hashing_queue.write(chunk)
                time.sleep(0.001)  # Simulate network delay
            hashing_queue.close()  # Signal end of data

        # Simulate uploader reading from queue
        uploaded_data = BytesIO()

        def uploader():
            while True:
                chunk = hashing_queue.read(2048)
                if not chunk:
                    break
                uploaded_data.write(chunk)

        # Start both threads
        download_thread = threading.Thread(target=downloader)
        upload_thread = threading.Thread(target=uploader)

        download_thread.start()
        upload_thread.start()

        download_thread.join()
        upload_thread.join()

        # Verify results
        self.assertEqual(uploaded_data.getvalue(), test_data)
        self.assertEqual(hashing_queue.get_hash(), expected_hash)
        self.assertEqual(hashing_queue.get_bytes_hashed(), len(test_data))

    def test_large_file_streaming_with_progress(self):
        """Test streaming a large file with hash and progress"""
        # Generate 10MB of test data
        file_size = 10 * 1024 * 1024
        test_data = os.urandom(file_size)
        expected_hash = hashlib.sha256(test_data).hexdigest()

        # Use limited queue with memory constraints
        hashing_queue = LimitedHashingQueueIO(
            memory_limit=2 * 1024 * 1024,  # 2MB limit for 10MB file
            chunk_size=512 * 1024,  # 512KB chunks
            show_progress=False,  # Disable for tests
        )

        bytes_downloaded = 0
        bytes_uploaded = 0

        def downloader():
            nonlocal bytes_downloaded
            # Simulate downloading in chunks
            chunk_size = 256 * 1024  # 256KB download chunks
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i : i + chunk_size]
                hashing_queue.write(chunk)
                bytes_downloaded += len(chunk)
                time.sleep(0.0001)  # Simulate network
            hashing_queue.close()

        def uploader():
            nonlocal bytes_uploaded
            while True:
                chunk = hashing_queue.read(128 * 1024)  # 128KB upload chunks
                if not chunk:
                    break
                bytes_uploaded += len(chunk)
                # Simulate upload processing
                time.sleep(0.0001)

        start_time = time.time()

        download_thread = threading.Thread(target=downloader)
        upload_thread = threading.Thread(target=uploader)

        download_thread.start()
        upload_thread.start()

        download_thread.join()
        upload_thread.join()

        duration = time.time() - start_time
        throughput_mbps = (file_size / (1024 * 1024)) / duration

        # Verify results
        self.assertEqual(bytes_downloaded, file_size)
        self.assertEqual(bytes_uploaded, file_size)
        self.assertEqual(hashing_queue.get_hash(), expected_hash)
        self.assertEqual(hashing_queue.get_bytes_hashed(), file_size)

        print(f"\nStreamed {file_size / (1024*1024):.1f}MB in {duration:.2f}s")
        print(f"Throughput: {throughput_mbps:.2f} MB/s")
        print(f"Hash: {hashing_queue.get_hash()[:16]}...")
        print(
            f"Memory limit: {hashing_queue._queue.maxsize * hashing_queue._chunk_size / (1024*1024):.1f}MB"
        )

    def test_multiple_hash_algorithms(self):
        """Test streaming with different hash algorithms"""
        test_data = b"Test data for multiple algorithms" * 100

        algorithms = ["md5", "sha1", "sha256", "sha512"]
        results = {}

        for algo in algorithms:
            # Create queue with specific algorithm
            hashing_queue = HashingQueueIO(hash_algorithm=algo)

            # Write data
            hashing_queue.write(test_data)
            hashing_queue.close()

            # Read data
            read_data = hashing_queue.read()

            # Store hash
            results[algo] = hashing_queue.get_hash()

            # Verify against standard library
            expected = hashlib.new(algo, test_data).hexdigest()
            self.assertEqual(results[algo], expected)

            print(f"{algo}: {results[algo][:16]}...")

    def test_concurrent_streams(self):
        """Test multiple concurrent hash streams"""
        # Create test files of different sizes
        files = [
            {"name": "small.bin", "size": 1 * 1024 * 1024},  # 1MB
            {"name": "medium.bin", "size": 5 * 1024 * 1024},  # 5MB
            {"name": "large.bin", "size": 10 * 1024 * 1024},  # 10MB
        ]

        # Generate test data
        for f in files:
            f["data"] = os.urandom(f["size"])
            f["expected_hash"] = hashlib.sha256(f["data"]).hexdigest()

        results = []
        threads = []

        def process_file(file_info):
            hashing_queue = HashingQueueIO(chunk_size=256 * 1024)  # 256KB chunks

            def download():
                # Write in chunks
                chunk_size = 128 * 1024
                data = file_info["data"]
                for i in range(0, len(data), chunk_size):
                    chunk = data[i : i + chunk_size]
                    hashing_queue.write(chunk)
                hashing_queue.close()

            def upload():
                total = 0
                while True:
                    chunk = hashing_queue.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)

                results.append(
                    {
                        "name": file_info["name"],
                        "size": total,
                        "hash": hashing_queue.get_hash(),
                    }
                )

            dl_thread = threading.Thread(target=download)
            ul_thread = threading.Thread(target=upload)

            dl_thread.start()
            ul_thread.start()

            dl_thread.join()
            ul_thread.join()

        # Process all files concurrently
        for file_info in files:
            thread = threading.Thread(target=process_file, args=(file_info,))
            thread.start()
            threads.append(thread)

        # Wait for all to complete
        for thread in threads:
            thread.join()

        # Verify results
        self.assertEqual(len(results), len(files))

        for file_info in files:
            result = next(r for r in results if r["name"] == file_info["name"])
            self.assertEqual(result["size"], file_info["size"])
            self.assertEqual(result["hash"], file_info["expected_hash"])
            print(f"✓ {result['name']}: {result['size'] / (1024*1024):.1f}MB, hash OK")


class LimitedHashingQueueIO(LimitedQueueIO):
    """Combined hashing and limited queue for memory-constrained streaming"""

    def __init__(self, hash_algorithm="sha256", **kwargs):
        # Initialize LimitedQueueIO
        super().__init__(**kwargs)

        # Add hashing functionality
        self._hasher = hashlib.new(hash_algorithm)
        self._hash_lock = threading.Lock()
        self._bytes_hashed = 0

    def write(self, b):
        """Write data and update hash"""
        # Update hash with the data
        with self._hash_lock:
            self._hasher.update(b)
            self._bytes_hashed += len(b)

        # Pass through to parent
        return super().write(b)

    def get_hash(self):
        """Get the computed hash (hexdigest)"""
        with self._hash_lock:
            return self._hasher.hexdigest()

    def get_bytes_hashed(self):
        """Get the number of bytes hashed"""
        with self._hash_lock:
            return self._bytes_hashed


if __name__ == "__main__":
    unittest.main()
