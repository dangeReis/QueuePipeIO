#!/usr/bin/env python3
"""
Demo of the new pipe-based architecture in QueuePipeIO.

This example shows how to use the new architecture to create pipelines
for data processing, similar to Unix pipes.
"""

import os
import sys
import threading
import time
import hashlib

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, HashingFilter

MB = 1024 * 1024


def example_1_basic_pipe():
    """Example 1: Basic pipe usage"""
    print("\n=== Example 1: Basic Pipe Usage ===")
    
    # Create pipe components
    writer = PipeWriter()
    reader = PipeReader()
    
    # Connect them
    writer.connect(reader)
    # Or use the pipe operator: writer | reader
    
    # Write data in one thread
    def write_data():
        data = b"Hello from the pipe!"
        print(f"Writing: {data}")
        writer.write(data)
        writer.close()
    
    # Read data in another thread
    def read_data():
        data = reader.read()
        print(f"Read: {data}")
    
    # Start threads
    write_thread = threading.Thread(target=write_data)
    read_thread = threading.Thread(target=read_data)
    
    write_thread.start()
    read_thread.start()
    
    write_thread.join()
    read_thread.join()


def example_2_memory_limited_pipe():
    """Example 2: Memory-limited pipe for large data transfers"""
    print("\n=== Example 2: Memory-Limited Pipe ===")
    
    # Create a pipe with 2MB memory limit
    writer = PipeWriter(memory_limit=2*MB, chunk_size=512*1024, show_progress=True)
    reader = PipeReader()
    writer | reader
    
    # Simulate large data transfer
    data_size = 5 * MB
    
    def producer():
        print(f"Producing {data_size/MB:.1f}MB of data...")
        chunk_size = 256 * 1024
        for i in range(0, data_size, chunk_size):
            chunk = b"x" * min(chunk_size, data_size - i)
            writer.write(chunk)
        writer.close()
    
    def consumer():
        total = 0
        while True:
            chunk = reader.read(128 * 1024)
            if not chunk:
                break
            total += len(chunk)
        print(f"Consumed {total/MB:.1f}MB of data")
    
    # Run transfer
    prod_thread = threading.Thread(target=producer)
    cons_thread = threading.Thread(target=consumer)
    
    prod_thread.start()
    cons_thread.start()
    
    prod_thread.join()
    cons_thread.join()


def example_3_hashing_pipeline():
    """Example 3: Pipeline with hash computation"""
    print("\n=== Example 3: Hashing Pipeline ===")
    
    # Create pipeline: writer -> hasher -> reader
    writer = PipeWriter()
    hasher = HashingFilter(algorithm='sha256')
    reader = PipeReader()
    
    # Connect using pipe operator
    pipeline = writer | hasher | reader
    
    # Test data
    test_data = b"Important data that needs integrity verification" * 100
    expected_hash = hashlib.sha256(test_data).hexdigest()
    
    def write_data():
        print(f"Writing {len(test_data)} bytes...")
        writer.write(test_data)
        writer.close()
    
    def read_and_verify():
        data = reader.read()
        computed_hash = hasher.get_hash()
        print(f"Read {len(data)} bytes")
        print(f"Expected hash:  {expected_hash}")
        print(f"Computed hash:  {computed_hash}")
        print(f"Hash match: {computed_hash == expected_hash}")
    
    # Run pipeline
    write_thread = threading.Thread(target=write_data)
    read_thread = threading.Thread(target=read_and_verify)
    
    write_thread.start()
    read_thread.start()
    
    write_thread.join()
    read_thread.join()


def example_4_s3_simulation():
    """Example 4: Simulate S3 download with verification"""
    print("\n=== Example 4: S3 Download Simulation ===")
    
    # Simulate downloading a 10MB file from S3 with hash verification
    file_size = 10 * MB
    file_data = os.urandom(file_size)
    s3_hash = hashlib.sha256(file_data).hexdigest()
    
    print(f"Simulating download of {file_size/MB:.1f}MB file")
    print(f"S3 provided hash: {s3_hash}")
    
    # Create pipeline: s3_writer -> hasher -> local_reader
    s3_writer = PipeWriter(memory_limit=5*MB, chunk_size=1*MB, show_progress=True)
    hasher = HashingFilter(algorithm='sha256')
    local_reader = PipeReader()
    
    s3_writer | hasher | local_reader
    
    def s3_download():
        """Simulate S3 downloading data"""
        chunk_size = 1 * MB
        for i in range(0, file_size, chunk_size):
            chunk = file_data[i:i+chunk_size]
            s3_writer.write(chunk)
            time.sleep(0.001)  # Simulate network latency
        s3_writer.close()
    
    def local_storage():
        """Simulate writing to local storage"""
        received = []
        while True:
            chunk = local_reader.read(512*1024)
            if not chunk:
                break
            received.append(chunk)
        
        # Verify
        total_size = sum(len(chunk) for chunk in received)
        computed_hash = hasher.get_hash()
        
        print(f"\nDownload complete:")
        print(f"  Total size: {total_size/MB:.1f}MB")
        print(f"  Computed hash: {computed_hash}")
        print(f"  Hash verification: {'PASSED' if computed_hash == s3_hash else 'FAILED'}")
    
    # Run simulation
    start_time = time.time()
    
    download_thread = threading.Thread(target=s3_download)
    storage_thread = threading.Thread(target=local_storage)
    
    download_thread.start()
    storage_thread.start()
    
    download_thread.join()
    storage_thread.join()
    
    duration = time.time() - start_time
    throughput = (file_size / MB) / duration
    print(f"  Throughput: {throughput:.2f} MB/s")


def example_5_chained_filters():
    """Example 5: Multiple filters in a pipeline"""
    print("\n=== Example 5: Chained Filters ===")
    
    # Create pipeline with multiple hashers
    writer = PipeWriter()
    sha256_hasher = HashingFilter(algorithm='sha256')
    md5_hasher = HashingFilter(algorithm='md5')
    reader = PipeReader()
    
    # Chain: writer -> sha256 -> md5 -> reader
    writer | sha256_hasher | md5_hasher | reader
    
    test_data = b"Data passing through multiple filters"
    
    # Write and read
    writer.write(test_data)
    writer.close()
    
    result = reader.read()
    
    print(f"Original data: {test_data}")
    print(f"Read data: {result}")
    print(f"SHA256: {sha256_hasher.get_hash()}")
    print(f"MD5: {md5_hasher.get_hash()}")
    print(f"Data preserved: {result == test_data}")


if __name__ == "__main__":
    print("QueuePipeIO New Architecture Demo")
    print("=================================")
    
    # Run all examples
    example_1_basic_pipe()
    example_2_memory_limited_pipe()
    example_3_hashing_pipeline()
    example_4_s3_simulation()
    example_5_chained_filters()
    
    print("\nDemo complete!")