#!/usr/bin/env python3
"""
Example: Read-Side Hashing with HashingLimitedQueueIO

This example demonstrates how to use HashingLimitedQueueIO for computing
hashes during read operations (consumer-side hashing).

Use cases:
1. Different consumers need different hash algorithms
2. Producer doesn't need to know about hashing
3. Hash verification on the consumer side
4. Multiple consumers with different integrity checks
"""

import threading
import time
import hashlib
import sys
import os

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import HashingLimitedQueueIO, LimitedQueueIO  # noqa: E402


def example_basic_usage():
    """Basic example of read-side hashing"""
    print("=== Basic Read-Side Hashing Example ===\n")

    # Create a hashing queue
    queue = HashingLimitedQueueIO(
        hash_algorithm="sha256",
        memory_limit=10 * 1024 * 1024,  # 10MB limit
        chunk_size=1024 * 1024,  # 1MB chunks
    )

    # Producer writes data (doesn't know about hashing)
    test_data = b"Hello, World! " * 100000  # ~1.4MB
    print(f"Writing {len(test_data) / 1024 / 1024:.2f}MB of data...")
    queue.write(test_data)
    queue.close()

    # Consumer reads data (hash computed during read)
    print("Reading data and computing hash...")
    read_data = queue.read()

    # Get the computed hash
    computed_hash = queue.get_hash()
    bytes_hashed = queue.get_bytes_hashed()

    print(f"Data read: {len(read_data) / 1024 / 1024:.2f}MB")
    print(f"Bytes hashed: {bytes_hashed / 1024 / 1024:.2f}MB")
    print(f"SHA256 hash: {computed_hash[:32]}...")
    print(f"Hash matches: {computed_hash == hashlib.sha256(test_data).hexdigest()}\n")


def example_s3_download_verification():
    """Example: Verifying S3 download integrity"""
    print("=== S3 Download Verification Example ===\n")

    # Simulate S3 metadata with expected hash
    s3_metadata = {
        "sha256": "5d41402abc4b2a76b9719d911017c592",  # Example hash
        "content-length": 50 * 1024 * 1024,  # 50MB
    }

    # Create hashing queue for verification
    queue = HashingLimitedQueueIO(
        hash_algorithm="sha256",
        memory_limit=20 * 1024 * 1024,  # 20MB limit for 50MB file
        chunk_size=5 * 1024 * 1024,  # 5MB chunks
    )

    download_complete = False
    verification_result = None

    def s3_downloader():
        """Simulate S3 download"""
        nonlocal download_complete
        print("Starting S3 download...")

        # Simulate downloading chunks from S3
        total_size = s3_metadata["content-length"]
        chunk_size = 1024 * 1024  # 1MB chunks
        downloaded = 0

        while downloaded < total_size:
            # Simulate chunk download
            chunk = b"X" * min(chunk_size, total_size - downloaded)
            queue.write(chunk)
            downloaded += len(chunk)

            # Progress
            if downloaded % (10 * 1024 * 1024) == 0:
                print(f"Downloaded {downloaded / 1024 / 1024:.0f}MB...")

            time.sleep(0.001)  # Simulate network delay

        queue.close()
        download_complete = True
        print("Download complete!")

    def local_storage_writer():
        """Write to local storage and verify hash"""
        nonlocal verification_result
        print("Writing to local storage and verifying...")

        total_read = 0
        while True:
            chunk = queue.read(512 * 1024)  # Read 512KB at a time
            if not chunk:
                if download_complete:
                    break
                time.sleep(0.001)
                continue

            # Simulate writing to disk
            total_read += len(chunk)

            # Progress
            if total_read % (10 * 1024 * 1024) == 0:
                print(f"Written {total_read / 1024 / 1024:.0f}MB to disk...")

        # Verify hash after all data is read
        computed_hash = queue.get_hash()
        # In real scenario, you'd compare with actual S3 hash
        verification_result = {
            "computed_hash": computed_hash,
            "bytes_processed": queue.get_bytes_hashed(),
            "verified": True,  # Would check against s3_metadata['sha256']
        }

        print("\nVerification complete!")
        print(
            f"Bytes processed: {verification_result['bytes_processed'] / 1024 / 1024:.1f}MB"
        )
        print(f"Hash computed: {verification_result['computed_hash'][:32]}...")
        print(
            f"Verification: {'PASSED' if verification_result['verified'] else 'FAILED'}"
        )

    # Run both operations concurrently
    download_thread = threading.Thread(target=s3_downloader)
    storage_thread = threading.Thread(target=local_storage_writer)

    start_time = time.time()
    download_thread.start()
    storage_thread.start()

    download_thread.join()
    storage_thread.join()

    duration = time.time() - start_time
    throughput = (s3_metadata["content-length"] / (1024 * 1024)) / duration
    print(f"\nTotal time: {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} MB/s\n")


def example_multiple_consumers():
    """Example: Multiple consumers with different hash algorithms"""
    print("=== Multiple Consumers Example ===\n")

    # Shared data
    shared_data = b"This is shared data that multiple consumers will process" * 10000

    # Consumer 1: Uses SHA256 for security
    def security_consumer():
        queue = HashingLimitedQueueIO(hash_algorithm="sha256")
        queue.write(shared_data)
        queue.close()

        # Read and compute SHA256
        _ = queue.read()  # Read all data to compute hash
        sha256_hash = queue.get_hash()
        print(f"Security consumer (SHA256): {sha256_hash[:32]}...")
        return sha256_hash

    # Consumer 2: Uses MD5 for legacy system compatibility
    def legacy_consumer():
        queue = HashingLimitedQueueIO(hash_algorithm="md5")
        queue.write(shared_data)
        queue.close()

        # Read and compute MD5
        _ = queue.read()  # Read all data to compute hash
        md5_hash = queue.get_hash()
        print(f"Legacy consumer (MD5): {md5_hash[:32]}...")
        return md5_hash

    # Consumer 3: Uses SHA-1 for Git-like operations
    def git_consumer():
        queue = HashingLimitedQueueIO(hash_algorithm="sha1")
        queue.write(shared_data)
        queue.close()

        # Read and compute SHA-1
        _ = queue.read()  # Read all data to compute hash
        sha1_hash = queue.get_hash()
        print(f"Git consumer (SHA-1): {sha1_hash[:32]}...")
        return sha1_hash

    # Run all consumers
    print("Processing data with different hash algorithms...")
    sha256 = security_consumer()
    md5 = legacy_consumer()
    sha1 = git_consumer()

    # Verify against direct computation
    print("\nVerifying hashes...")
    print(f"SHA256 correct: {sha256 == hashlib.sha256(shared_data).hexdigest()}")
    print(f"MD5 correct: {md5 == hashlib.md5(shared_data).hexdigest()}")
    print(f"SHA1 correct: {sha1 == hashlib.sha1(shared_data).hexdigest()}\n")


def example_comparison_read_vs_write_hashing():
    """Compare read-side vs write-side hashing"""
    print("=== Read-Side vs Write-Side Hashing Comparison ===\n")

    test_data = b"X" * (10 * 1024 * 1024)  # 10MB

    # Method 1: Read-side hashing (HashingLimitedQueueIO)
    print("Method 1: Read-side hashing (consumer computes hash)")
    start_time = time.time()

    read_queue = HashingLimitedQueueIO(
        memory_limit=5 * 1024 * 1024, chunk_size=1024 * 1024
    )

    # Producer doesn't compute hash
    read_queue.write(test_data)
    read_queue.close()

    # Consumer computes hash during read
    _ = read_queue.read()
    read_side_hash = read_queue.get_hash()
    read_side_time = time.time() - start_time

    print(f"Time: {read_side_time:.3f}s")
    print(f"Hash: {read_side_hash[:32]}...")

    # Method 2: Write-side hashing (manual)
    print("\nMethod 2: Write-side hashing (producer computes hash)")
    start_time = time.time()

    write_queue = LimitedQueueIO(memory_limit=5 * 1024 * 1024, chunk_size=1024 * 1024)

    # Producer computes hash
    hasher = hashlib.sha256()
    hasher.update(test_data)
    write_side_hash = hasher.hexdigest()

    write_queue.write(test_data)
    write_queue.close()

    # Consumer just reads
    _ = write_queue.read()
    write_side_time = time.time() - start_time

    print(f"Time: {write_side_time:.3f}s")
    print(f"Hash: {write_side_hash[:32]}...")

    print(f"\nHashes match: {read_side_hash == write_side_hash}")
    print(f"Performance difference: {abs(read_side_time - write_side_time):.3f}s")

    print("\nWhen to use each approach:")
    print(
        "- Read-side: When consumers need different hashes or producer shouldn't know about hashing"
    )
    print("- Write-side: When hash is computed once and shared by all consumers\n")


def main():
    """Run all examples"""
    print("HashingLimitedQueueIO Examples - Read-Side Hashing\n")
    print("This demonstrates computing hashes during read operations.\n")

    example_basic_usage()
    example_s3_download_verification()
    example_multiple_consumers()
    example_comparison_read_vs_write_hashing()

    print("All examples completed!")


if __name__ == "__main__":
    main()
