#!/usr/bin/env python3
"""
Tests for pipe filters including HashingFilter

This module tests the filter functionality in the new pipe architecture.
"""

import unittest
import threading
import hashlib
import time
import os
import sys

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, HashingFilter, CopyFilter

MB = 1024 * 1024


class TestHashingFilter(unittest.TestCase):
    """Test HashingFilter functionality"""

    def test_basic_hash_computation(self):
        """Test basic hash computation through filter"""
        test_data = b"Hello, World! This is a test of HashingFilter."
        expected_hash = hashlib.sha256(test_data).hexdigest()

        # Create pipeline: writer -> hasher -> reader
        writer = PipeWriter()
        hasher = HashingFilter()
        reader = PipeReader()
        
        # Connect components
        pipeline = writer | hasher | reader
        
        # Write data
        writer.write(test_data)
        writer.close()
        
        # Read data
        read_data = reader.read()
        
        # Verify
        self.assertEqual(read_data, test_data)
        self.assertEqual(hasher.get_hash(), expected_hash)
        self.assertEqual(hasher.get_bytes_hashed(), len(test_data))

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
                
                # Create pipeline with specific algorithm
                writer = PipeWriter()
                hasher = HashingFilter(algorithm=algo)
                reader = PipeReader()
                
                writer | hasher | reader
                
                # Write and close
                writer.write(test_data)
                writer.close()
                
                # Read all data
                read_data = reader.read()
                
                # Verify hash
                expected = hashlib.new(algo, test_data).hexdigest()
                self.assertEqual(hasher.get_hash(), expected)
                self.assertEqual(read_data, test_data)

    def test_streaming_hash(self):
        """Test hash computation with streaming data"""
        # Generate test data
        data_size = 5 * MB
        test_data = os.urandom(data_size)
        expected_hash = hashlib.sha256(test_data).hexdigest()
        
        # Create pipeline with memory limit
        writer = PipeWriter(memory_limit=1*MB, chunk_size=256*1024)
        hasher = HashingFilter(chunk_size=128*1024)
        reader = PipeReader()
        
        writer | hasher | reader
        
        # Producer thread
        def producer():
            chunk_size = 512 * 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i:i+chunk_size]
                writer.write(chunk)
            writer.close()
        
        # Consumer thread
        read_data = []
        def consumer():
            while True:
                chunk = reader.read(64 * 1024)
                if not chunk:
                    break
                read_data.append(chunk)
        
        # Run threads
        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)
        
        start_time = time.time()
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join()
        duration = time.time() - start_time
        
        # Verify
        self.assertEqual(b"".join(read_data), test_data)
        self.assertEqual(hasher.get_hash(), expected_hash)
        self.assertEqual(hasher.get_bytes_hashed(), data_size)
        
        throughput = (data_size / MB) / duration
        print(f"\nStreaming throughput: {throughput:.2f} MB/s")

    def test_reset_hash(self):
        """Test resetting hash computation"""
        data1 = b"First data set"
        data2 = b"Second data set"
        
        # First operation
        writer = PipeWriter()
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        writer.write(data1)
        writer.close()
        _ = reader.read()
        hash1 = hasher.get_hash()
        
        # For second operation, create new pipeline
        # (filters can't be reused in different pipelines)
        writer2 = PipeWriter()
        hasher2 = HashingFilter()
        reader2 = PipeReader()
        writer2 | hasher2 | reader2
        
        writer2.write(data2)
        writer2.close()
        _ = reader2.read()
        hash2 = hasher2.get_hash()
        
        # Verify hashes
        self.assertNotEqual(hash1, hash2)
        self.assertEqual(hash1, hashlib.sha256(data1).hexdigest())
        self.assertEqual(hash2, hashlib.sha256(data2).hexdigest())
        
        # Test reset on same hasher
        hasher.reset_hash()
        self.assertEqual(hasher.get_bytes_hashed(), 0)

    def test_chained_filters(self):
        """Test chaining multiple filters"""
        test_data = b"Test data for chained filters" * 100
        
        # Create pipeline: writer -> hasher1 -> hasher2 -> reader
        writer = PipeWriter()
        hasher1 = HashingFilter(algorithm='sha256')
        hasher2 = HashingFilter(algorithm='md5')
        reader = PipeReader()
        
        writer | hasher1 | hasher2 | reader
        
        # Write data
        writer.write(test_data)
        writer.close()
        
        # Read data
        read_data = reader.read()
        
        # Verify both hashes
        self.assertEqual(read_data, test_data)
        self.assertEqual(hasher1.get_hash(), hashlib.sha256(test_data).hexdigest())
        self.assertEqual(hasher2.get_hash(), hashlib.md5(test_data).hexdigest())


class TestCopyFilter(unittest.TestCase):
    """Test CopyFilter functionality"""
    
    def test_copy_filter(self):
        """Test basic copy filter operation"""
        test_data = b"Data to be copied through filter"
        
        writer = PipeWriter()
        copier = CopyFilter()
        reader = PipeReader()
        
        writer | copier | reader
        
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)


class TestS3Simulation(unittest.TestCase):
    """Integration tests simulating S3 operations"""
    
    def test_s3_download_with_verification(self):
        """Simulate S3 download with hash verification"""
        # Simulate a file with known hash
        file_size = 10 * MB
        file_data = os.urandom(file_size)
        expected_hash = hashlib.sha256(file_data).hexdigest()
        
        # Simulate S3 metadata
        s3_metadata = {"sha256": expected_hash}
        
        # Create pipeline: s3_writer -> hasher -> local_reader
        s3_writer = PipeWriter(memory_limit=5*MB, chunk_size=1*MB)
        hasher = HashingFilter(algorithm='sha256')
        local_reader = PipeReader()
        
        s3_writer | hasher | local_reader
        
        download_complete = False
        verification_passed = False
        
        def s3_download():
            """Simulate S3 download"""
            nonlocal download_complete
            chunk_size = 1 * MB
            for i in range(0, len(file_data), chunk_size):
                chunk = file_data[i:i+chunk_size]
                s3_writer.write(chunk)
                time.sleep(0.0001)  # Simulate network delay
            s3_writer.close()
            download_complete = True
        
        def local_storage():
            """Simulate local storage with verification"""
            nonlocal verification_passed
            received_data = []
            
            while True:
                chunk = local_reader.read(512 * 1024)
                if not chunk:
                    break
                received_data.append(chunk)
            
            # Verify hash
            computed_hash = hasher.get_hash()
            verification_passed = computed_hash == s3_metadata["sha256"]
            
            # Verify data integrity
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
        self.assertEqual(hasher.get_bytes_hashed(), file_size)
        
        throughput = (file_size / MB) / duration
        print(f"\nS3 simulation throughput: {throughput:.2f} MB/s")
        print(f"Hash verification: {'PASSED' if verification_passed else 'FAILED'}")

    def test_s3_upload_with_preprocessing(self):
        """Simulate S3 upload with hash computation"""
        # Generate data to upload
        file_size = 10 * MB
        file_data = os.urandom(file_size)
        
        # Create pipeline: local_writer -> hasher -> s3_reader
        local_writer = PipeWriter(memory_limit=5*MB, chunk_size=1*MB)
        hasher = HashingFilter(algorithm='sha256')
        s3_reader = PipeReader()
        
        local_writer | hasher | s3_reader
        
        upload_complete = False
        uploaded_data = []
        
        def local_read():
            """Simulate reading from local file"""
            chunk_size = 1 * MB
            for i in range(0, len(file_data), chunk_size):
                chunk = file_data[i:i+chunk_size]
                local_writer.write(chunk)
            local_writer.close()
        
        def s3_upload():
            """Simulate S3 upload"""
            nonlocal upload_complete
            while True:
                chunk = s3_reader.read(2 * MB)  # S3 multipart chunks
                if not chunk:
                    break
                uploaded_data.append(chunk)
                time.sleep(0.0001)  # Simulate network delay
            upload_complete = True
        
        # Run simulation
        read_thread = threading.Thread(target=local_read)
        upload_thread = threading.Thread(target=s3_upload)
        
        start_time = time.time()
        read_thread.start()
        upload_thread.start()
        
        read_thread.join()
        upload_thread.join()
        duration = time.time() - start_time
        
        # Verify
        self.assertEqual(b"".join(uploaded_data), file_data)
        computed_hash = hasher.get_hash()
        expected_hash = hashlib.sha256(file_data).hexdigest()
        self.assertEqual(computed_hash, expected_hash)
        
        throughput = (file_size / MB) / duration
        print(f"\nS3 upload throughput: {throughput:.2f} MB/s")
        print(f"Computed hash: {computed_hash}")


if __name__ == "__main__":
    unittest.main()