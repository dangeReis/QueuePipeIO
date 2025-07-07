#!/usr/bin/env python3
"""
Comprehensive tests for the new pipe-based architecture of QueuePipeIO.

This module tests all aspects of the pipe architecture including:
- Basic pipe operations
- Memory limits and backpressure
- Error handling
- Thread safety
- Performance characteristics
"""

import unittest
import threading
import time
import os
import sys
import queue
from unittest.mock import patch

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, HashingFilter, CopyFilter

MB = 1024 * 1024


class TestPipeBasics(unittest.TestCase):
    """Test basic pipe operations"""

    def test_simple_write_read(self):
        """Test basic write and read operations"""
        writer = PipeWriter()
        reader = PipeReader()
        writer.connect(reader)
        
        test_data = b"Hello, Pipe World!"
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)

    def test_pipe_operator_syntax(self):
        """Test the | operator for connecting pipes"""
        writer = PipeWriter()
        reader = PipeReader()
        
        # Test chaining
        result = writer | reader
        self.assertIs(result, reader)
        
        # Verify connection
        writer.write(b"test")
        writer.close()
        self.assertEqual(reader.read(), b"test")

    def test_multiple_writes(self):
        """Test multiple write operations"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        for chunk in chunks:
            writer.write(chunk)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, b"".join(chunks))

    def test_partial_reads(self):
        """Test reading specific number of bytes"""
        writer = PipeWriter(chunk_size=10)
        reader = PipeReader()
        writer | reader
        
        test_data = b"0123456789ABCDEFGHIJ"
        writer.write(test_data)
        writer.close()
        
        # Read in parts
        part1 = reader.read(5)
        part2 = reader.read(5)
        part3 = reader.read()  # Read rest
        
        self.assertEqual(part1, b"01234")
        self.assertEqual(part2, b"56789")
        self.assertEqual(part3, b"ABCDEFGHIJ")

    def test_empty_read(self):
        """Test reading from empty pipe"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        writer.close()  # Close without writing
        result = reader.read()
        self.assertEqual(result, b"")

    def test_zero_byte_read(self):
        """Test read(0) returns empty bytes"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        writer.write(b"data")
        result = reader.read(0)
        self.assertEqual(result, b"")
        writer.close()


class TestPipeChunking(unittest.TestCase):
    """Test chunking behavior in pipes"""

    def test_small_chunk_size(self):
        """Test with very small chunk size"""
        writer = PipeWriter(chunk_size=5)
        reader = PipeReader()
        writer | reader
        
        # Write data larger than chunk
        test_data = b"0123456789"
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)

    def test_exact_chunk_boundary(self):
        """Test writing exactly chunk_size bytes"""
        chunk_size = 10
        writer = PipeWriter(chunk_size=chunk_size)
        reader = PipeReader()
        writer | reader
        
        test_data = b"X" * chunk_size
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)

    def test_multiple_chunk_accumulation(self):
        """Test accumulating data across multiple chunks"""
        chunk_size = 8
        writer = PipeWriter(chunk_size=chunk_size)
        reader = PipeReader()
        writer | reader
        
        # Write in small pieces that accumulate
        for i in range(5):
            writer.write(b"XX")  # 2 bytes each
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, b"XX" * 5)
        self.assertEqual(len(result), 10)


class TestMemoryLimits(unittest.TestCase):
    """Test memory limit functionality"""

    def test_memory_limit_queue_size(self):
        """Test that memory limit correctly sets queue size"""
        memory_limit = 1 * MB
        chunk_size = 256 * 1024  # 256KB
        
        writer = PipeWriter(memory_limit=memory_limit, chunk_size=chunk_size)
        reader = PipeReader()
        writer | reader
        
        # Queue size should be memory_limit / chunk_size = 4
        expected_queue_size = memory_limit // chunk_size
        self.assertEqual(writer._queue.maxsize, expected_queue_size)

    def test_memory_limit_backpressure(self):
        """Test that memory limit causes backpressure"""
        memory_limit = 512 * 1024  # 512KB
        chunk_size = 256 * 1024    # 256KB chunks
        
        writer = PipeWriter(memory_limit=memory_limit, chunk_size=chunk_size)
        reader = PipeReader()
        writer | reader
        
        # Write 1MB of data (4 chunks)
        data = b"X" * (1 * MB)
        
        write_complete = threading.Event()
        
        def write_data():
            writer.write(data)
            writer.close()
            write_complete.set()
        
        # Start writing in thread
        write_thread = threading.Thread(target=write_data)
        write_thread.start()
        
        # Give writer time to fill queue
        time.sleep(0.1)
        
        # Writer should be blocked
        self.assertFalse(write_complete.is_set())
        
        # Start reading to unblock
        _ = reader.read(512 * 1024)  # Read half
        
        # Give time for writer to progress
        time.sleep(0.1)
        
        # Read rest
        _ = reader.read()
        
        write_thread.join(timeout=2.0)
        self.assertTrue(write_complete.is_set())

    def test_no_memory_limit(self):
        """Test unlimited memory (no backpressure)"""
        writer = PipeWriter(memory_limit=None)
        reader = PipeReader()
        writer | reader
        
        # Should have unlimited queue
        self.assertEqual(writer._queue.maxsize, 0)  # 0 means unlimited
        
        # Write large amount without blocking
        large_data = b"X" * (10 * MB)
        writer.write(large_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(len(result), len(large_data))


class TestErrorHandling(unittest.TestCase):
    """Test error conditions and edge cases"""

    def test_writer_not_connected(self):
        """Test error when writer is not connected"""
        writer = PipeWriter()
        
        with self.assertRaises(RuntimeError) as cm:
            writer.write(b"data")
        self.assertIn("not connected", str(cm.exception))

    def test_reader_not_connected(self):
        """Test error when reader is not connected"""
        reader = PipeReader()
        
        with self.assertRaises(RuntimeError) as cm:
            reader.read()
        self.assertIn("not connected", str(cm.exception))

    def test_double_connect(self):
        """Test error when connecting writer twice"""
        writer = PipeWriter()
        reader1 = PipeReader()
        reader2 = PipeReader()
        
        writer.connect(reader1)
        
        with self.assertRaises(RuntimeError) as cm:
            writer.connect(reader2)
        self.assertIn("already connected", str(cm.exception))

    def test_write_after_close(self):
        """Test error when writing after close"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        writer.close()
        
        with self.assertRaises(ValueError) as cm:
            writer.write(b"data")
        self.assertIn("closed file", str(cm.exception))

    def test_read_after_close(self):
        """Test error when reading after close"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        reader.close()
        
        with self.assertRaises(ValueError) as cm:
            reader.read()
        self.assertIn("closed file", str(cm.exception))

    def test_invalid_chunk_size(self):
        """Test invalid chunk sizes"""
        with self.assertRaises(ValueError):
            PipeWriter(chunk_size=0)
        
        with self.assertRaises(ValueError):
            PipeWriter(chunk_size=-1)

    def test_invalid_memory_limit(self):
        """Test invalid memory limits"""
        with self.assertRaises(ValueError):
            PipeWriter(memory_limit=-1)
        
        with self.assertRaises(ValueError):
            PipeWriter(memory_limit=0)

    def test_write_non_bytes(self):
        """Test writing non-bytes data"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        with self.assertRaises(TypeError):
            writer.write("string")
        
        with self.assertRaises(TypeError):
            writer.write(123)
        
        with self.assertRaises(TypeError):
            writer.write(None)


class TestThreadSafety(unittest.TestCase):
    """Test thread safety of pipe operations"""

    def test_concurrent_read_write(self):
        """Test concurrent reading and writing"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        data_size = 1 * MB
        test_data = os.urandom(data_size)
        
        result_data = []
        write_complete = threading.Event()
        
        def writer_thread():
            chunk_size = 64 * 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i:i+chunk_size]
                writer.write(chunk)
                time.sleep(0.0001)  # Simulate work
            writer.close()
            write_complete.set()
        
        def reader_thread():
            while True:
                chunk = reader.read(32 * 1024)
                if not chunk:
                    if write_complete.is_set():
                        break
                    time.sleep(0.0001)
                else:
                    result_data.append(chunk)
        
        # Start threads
        w_thread = threading.Thread(target=writer_thread)
        r_thread = threading.Thread(target=reader_thread)
        
        w_thread.start()
        r_thread.start()
        
        w_thread.join()
        r_thread.join()
        
        # Verify data integrity
        self.assertEqual(b"".join(result_data), test_data)

    def test_multiple_readers_not_allowed(self):
        """Test that multiple readers on same queue fails"""
        writer = PipeWriter()
        reader1 = PipeReader()
        reader2 = PipeReader()
        
        writer | reader1
        
        # Second reader should fail to connect to same writer
        with self.assertRaises(RuntimeError):
            writer | reader2

    def test_reader_blocks_until_data(self):
        """Test that reader blocks when no data available"""
        writer = PipeWriter(chunk_size=10)  # Ensure data is sent immediately
        reader = PipeReader()
        writer | reader
        
        read_result = None
        read_complete = threading.Event()
        
        def reader_thread():
            nonlocal read_result
            read_result = reader.read(10)
            read_complete.set()
        
        # Start reader
        r_thread = threading.Thread(target=reader_thread)
        r_thread.start()
        
        # Reader should block
        time.sleep(0.1)
        self.assertFalse(read_complete.is_set())
        
        # Write exact chunk size to ensure it's sent
        writer.write(b"Hello12345")
        
        # Reader should unblock
        r_thread.join(timeout=2.0)
        self.assertTrue(read_complete.is_set())
        self.assertEqual(read_result, b"Hello12345")
        
        writer.close()


class TestFilters(unittest.TestCase):
    """Test filter functionality"""

    def test_single_filter_pipeline(self):
        """Test pipeline with single filter"""
        writer = PipeWriter()
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        test_data = b"Test data for hashing"
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        
        # Data should pass through unchanged
        self.assertEqual(result, test_data)
        
        # Hash should be computed
        import hashlib
        expected_hash = hashlib.sha256(test_data).hexdigest()
        self.assertEqual(hasher.get_hash(), expected_hash)

    def test_multiple_filters_pipeline(self):
        """Test pipeline with multiple filters"""
        writer = PipeWriter()
        hasher1 = HashingFilter(algorithm='sha256')
        hasher2 = HashingFilter(algorithm='md5')
        reader = PipeReader()
        
        writer | hasher1 | hasher2 | reader
        
        test_data = b"Data through multiple filters"
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)
        
        # Both hashes should be computed
        import hashlib
        self.assertEqual(hasher1.get_hash(), hashlib.sha256(test_data).hexdigest())
        self.assertEqual(hasher2.get_hash(), hashlib.md5(test_data).hexdigest())

    def test_filter_with_memory_limit(self):
        """Test filter in memory-limited pipeline"""
        writer = PipeWriter(memory_limit=1*MB, chunk_size=256*1024)
        hasher = HashingFilter()
        reader = PipeReader()
        
        writer | hasher | reader
        
        # Write 2MB of data
        test_data = os.urandom(2 * MB)
        
        write_complete = threading.Event()
        
        def write_thread():
            writer.write(test_data)
            writer.close()
            write_complete.set()
        
        def read_thread():
            chunks = []
            while True:
                chunk = reader.read(128 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        
        # Start threads
        w_thread = threading.Thread(target=write_thread)
        r_thread = threading.Thread(target=read_thread)
        
        w_thread.start()
        r_thread.start()
        
        w_thread.join()
        r_thread.join()
        
        # Verify hash
        import hashlib
        expected_hash = hashlib.sha256(test_data).hexdigest()
        self.assertEqual(hasher.get_hash(), expected_hash)
        self.assertEqual(hasher.get_bytes_hashed(), len(test_data))


class TestPipeStateManagement(unittest.TestCase):
    """Test pipe state management"""

    def test_readable_writable_state(self):
        """Test readable/writable state methods"""
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        
        # Initial state
        self.assertTrue(writer.writable())
        self.assertFalse(writer.readable())
        self.assertTrue(reader.readable())
        self.assertFalse(reader.writable())
        
        # After close
        writer.close()
        self.assertFalse(writer.writable())
        
        reader.close()
        self.assertFalse(reader.readable())

    def test_closed_property(self):
        """Test closed property"""
        writer = PipeWriter()
        reader = PipeReader()
        
        self.assertFalse(writer.closed)
        self.assertFalse(reader.closed)
        
        writer.close()
        self.assertTrue(writer.closed)
        
        reader.close()
        self.assertTrue(reader.closed)

    def test_seekable_always_false(self):
        """Test that pipes are never seekable"""
        writer = PipeWriter()
        reader = PipeReader()
        
        self.assertFalse(writer.seekable())
        self.assertFalse(reader.seekable())


class TestProgressBar(unittest.TestCase):
    """Test progress bar functionality"""

    @patch('queuepipeio.tqdm')
    def test_progress_bar_creation(self, mock_tqdm):
        """Test progress bar is created when requested"""
        writer = PipeWriter(memory_limit=1*MB, show_progress=True)
        reader = PipeReader()
        writer | reader
        
        # Progress bar should be created
        mock_tqdm.assert_called_once()
        
        writer.close()

    def test_no_progress_without_memory_limit(self):
        """Test progress bar requires memory limit"""
        writer = PipeWriter(show_progress=True)  # No memory limit
        reader = PipeReader()
        writer | reader
        
        # No progress bar without memory limit
        self.assertIsNone(writer.progress_bar)
        
        writer.close()


if __name__ == "__main__":
    unittest.main()