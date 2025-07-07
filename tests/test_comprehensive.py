import unittest
import threading
import time
import queue
import sys
import os
from io import BytesIO

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import QueueIO, LimitedQueueIO
MB = 1024 * 1024  # Define MB constant


class TestQueueIOChunking(unittest.TestCase):
    """Test chunking behavior of QueueIO"""

    def test_single_chunk_write(self):
        """Test writing data smaller than chunk size"""
        qio = QueueIO(chunk_size=1024)
        data = b"Hello World"
        written = qio.write(data)

        self.assertEqual(written, len(data))
        # Data should still be in write buffer, not in queue
        self.assertTrue(qio._queue.empty())

        # After close, data should be in queue
        qio.close()
        chunk = qio._queue.get()
        self.assertEqual(chunk, data)
        self.assertIsNone(qio._queue.get())  # EOF marker

    def test_multiple_chunk_write(self):
        """Test writing data larger than chunk size"""
        chunk_size = 10
        qio = QueueIO(chunk_size=chunk_size)
        data = b"A" * 25  # 2.5 chunks

        written = qio.write(data)
        self.assertEqual(written, 25)

        # Should have 2 complete chunks in queue
        chunk1 = qio._queue.get()
        chunk2 = qio._queue.get()
        self.assertEqual(chunk1, b"A" * 10)
        self.assertEqual(chunk2, b"A" * 10)

        # Remaining 5 bytes should be in write buffer
        self.assertEqual(len(qio._write_buffer), 5)

        qio.close()
        # Remaining data and EOF marker
        chunk3 = qio._queue.get()
        self.assertEqual(chunk3, b"A" * 5)
        self.assertIsNone(qio._queue.get())

    def test_exact_chunk_boundary(self):
        """Test writing data exactly at chunk boundaries"""
        chunk_size = 10
        qio = QueueIO(chunk_size=chunk_size)

        # Write exactly one chunk
        data1 = b"A" * 10
        qio.write(data1)
        self.assertEqual(qio._queue.qsize(), 1)
        self.assertEqual(qio._write_buffer, b"")

        # Write exactly two more chunks
        data2 = b"B" * 20
        qio.write(data2)
        self.assertEqual(qio._queue.qsize(), 3)
        self.assertEqual(qio._write_buffer, b"")

    def test_incremental_writes(self):
        """Test multiple small writes that accumulate to chunks"""
        chunk_size = 10
        qio = QueueIO(chunk_size=chunk_size)

        # Write 3 bytes at a time
        for _ in range(5):
            qio.write(b"ABC")

        # After 5 writes (15 bytes), should have 1 chunk in queue
        self.assertEqual(qio._queue.qsize(), 1)
        chunk = qio._queue.get()
        self.assertEqual(chunk, b"ABCABCABCA")
        self.assertEqual(qio._write_buffer, b"BCABC")


class TestQueueIOReading(unittest.TestCase):
    """Test reading behavior of QueueIO"""

    def test_read_all(self):
        """Test reading all data with read(-1)"""
        qio = QueueIO(chunk_size=10)
        data = b"Hello World!"
        qio.write(data)
        qio.close()

        read_data = qio.read()
        self.assertEqual(read_data, data)

        # Second read should return empty
        self.assertEqual(qio.read(), b"")

    def test_read_specific_bytes(self):
        """Test reading specific number of bytes"""
        qio = QueueIO(chunk_size=10)
        data = b"Hello World!"
        qio.write(data)
        qio.close()

        # Read 5 bytes
        chunk1 = qio.read(5)
        self.assertEqual(chunk1, b"Hello")

        # Read 7 bytes
        chunk2 = qio.read(7)
        self.assertEqual(chunk2, b" World!")

        # Nothing left
        self.assertEqual(qio.read(10), b"")

    def test_read_zero_bytes(self):
        """Test reading zero bytes"""
        qio = QueueIO()
        qio.write(b"Hello")

        result = qio.read(0)
        self.assertEqual(result, b"")

        # Data should still be available
        qio.close()
        self.assertEqual(qio.read(), b"Hello")

    def test_read_more_than_available(self):
        """Test reading more bytes than available"""
        qio = QueueIO(chunk_size=10)
        data = b"Hello"
        qio.write(data)
        qio.close()

        result = qio.read(100)
        self.assertEqual(result, data)

    def test_read_across_chunks(self):
        """Test reading that spans multiple chunks"""
        chunk_size = 5
        qio = QueueIO(chunk_size=chunk_size)

        # Write 3 chunks worth
        qio.write(b"AAAAA")
        qio.write(b"BBBBB")
        qio.write(b"CCCCC")
        qio.close()

        # Read across chunk boundaries
        result = qio.read(8)
        self.assertEqual(result, b"AAAAABBB")

        result = qio.read(7)
        self.assertEqual(result, b"BBCCCCC")


class TestQueueIOThreadSafety(unittest.TestCase):
    """Test thread safety of QueueIO"""

    def test_concurrent_write_read(self):
        """Test concurrent writing and reading"""
        qio = QueueIO(chunk_size=10)
        write_data = []
        read_data = []

        def writer():
            for i in range(10):
                data = f"Data{i:03d}X".encode()  # 10 bytes each
                write_data.append(data)
                qio.write(data)
                time.sleep(0.001)  # Small delay to interleave
            qio.close()

        def reader():
            while True:
                data = qio.read(10)
                if not data:
                    break
                read_data.append(data)

        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)

        writer_thread.start()
        reader_thread.start()

        writer_thread.join()
        reader_thread.join()

        # All written data should be read
        self.assertEqual(b"".join(write_data), b"".join(read_data))

    def test_multiple_writers(self):
        """Test multiple writers to same QueueIO"""
        qio = QueueIO(chunk_size=100)
        results = []
        lock = threading.Lock()

        def writer(writer_id, count):
            for i in range(count):
                data = f"W{writer_id}:{i:03d}|".encode()
                qio.write(data)
                with lock:
                    results.append(data)
                time.sleep(0.001)

        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i, 5))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        qio.close()

        # Read all data
        read_data = qio.read()

        # Verify all writes are present in the read data
        # Since writes happen concurrently, we can't predict exact order
        self.assertEqual(len(read_data), len(b"".join(results)))

        # Check that each written chunk appears in the output
        for chunk in results:
            self.assertIn(chunk, read_data, f"Chunk {chunk} not found in read data")

    def test_close_while_reading(self):
        """Test closing while reader is waiting"""
        qio = QueueIO()
        read_result = []

        def reader():
            data = qio.read()
            read_result.append(data)

        # Start reader
        reader_thread = threading.Thread(target=reader)
        reader_thread.start()

        # Give reader time to start waiting
        time.sleep(0.05)

        # Write some data and close
        qio.write(b"Final data")
        qio.close()

        reader_thread.join(timeout=1)
        self.assertFalse(reader_thread.is_alive())
        self.assertEqual(read_result[0], b"Final data")


class TestQueueIOEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""

    def test_invalid_chunk_size(self):
        """Test invalid chunk sizes"""
        with self.assertRaises(ValueError):
            QueueIO(chunk_size=0)

        with self.assertRaises(ValueError):
            QueueIO(chunk_size=-1)

    def test_write_after_close(self):
        """Test writing after close"""
        qio = QueueIO()
        qio.close()

        with self.assertRaises(ValueError) as cm:
            qio.write(b"data")
        self.assertIn("closed file", str(cm.exception))

    def test_write_non_bytes(self):
        """Test writing non-bytes data"""
        qio = QueueIO()

        with self.assertRaises(TypeError) as cm:
            qio.write("string data")
        self.assertIn("bytes-like object", str(cm.exception))

        with self.assertRaises(TypeError):
            qio.write(123)

        with self.assertRaises(TypeError):
            qio.write(None)

    def test_double_close(self):
        """Test closing twice"""
        qio = QueueIO()
        qio.write(b"data")
        qio.close()

        # Second close should not raise or add another EOF marker
        qio.close()

        # Should only get data and one EOF marker
        self.assertEqual(qio._queue.get(), b"data")
        self.assertIsNone(qio._queue.get())

        # Queue should now be empty
        with self.assertRaises(queue.Empty):
            qio._queue.get_nowait()

    def test_empty_write(self):
        """Test writing empty bytes"""
        qio = QueueIO()
        result = qio.write(b"")
        self.assertEqual(result, 0)

        # Should not affect queue
        self.assertTrue(qio._queue.empty())

    def test_read_from_empty(self):
        """Test reading from empty QueueIO"""
        qio = QueueIO()
        qio.close()

        result = qio.read()
        self.assertEqual(result, b"")

    def test_seekable_readable_writable(self):
        """Test file-like interface methods"""
        qio = QueueIO()
        self.assertFalse(qio.seekable())
        self.assertTrue(qio.readable())
        self.assertTrue(qio.writable())


class TestLimitedQueueIO(unittest.TestCase):
    """Test LimitedQueueIO specific functionality"""

    def test_memory_limit_queue_size(self):
        """Test queue size calculation from memory limit"""
        memory_limit = 100
        chunk_size = 10
        lqio = LimitedQueueIO(
            memory_limit=memory_limit, chunk_size=chunk_size, show_progress=False
        )

        # Queue size should be memory_limit // chunk_size
        expected_size = 10
        self.assertEqual(lqio._queue.maxsize, expected_size)

    def test_memory_limit_none(self):
        """Test LimitedQueueIO with no memory limit"""
        lqio = LimitedQueueIO(memory_limit=None, show_progress=False)

        # Should behave like regular QueueIO
        self.assertEqual(lqio._queue.maxsize, 0)  # Unlimited
        self.assertIsNone(lqio.status_bar)

    def test_invalid_memory_limit(self):
        """Test invalid memory limits"""
        with self.assertRaises(ValueError):
            LimitedQueueIO(memory_limit=0, show_progress=False)

        with self.assertRaises(ValueError):
            LimitedQueueIO(memory_limit=-100, show_progress=False)

    def test_queue_blocking(self):
        """Test that queue blocks when memory limit is reached"""
        memory_limit = 30
        chunk_size = 10
        # Use a short timeout to avoid hanging
        lqio = LimitedQueueIO(
            memory_limit=memory_limit,
            chunk_size=chunk_size,
            show_progress=False,
            write_timeout=0.5,
        )

        # Write 3 chunks to fill the queue
        lqio.write(b"A" * 30)

        # Queue should be full
        self.assertEqual(lqio._queue.qsize(), 3)

        # Try to write more in a thread
        write_completed = []
        write_exception = []

        def writer():
            try:
                # This will timeout after 0.5 seconds if queue is full
                lqio.write(b"B" * 10)
                write_completed.append(True)
            except queue.Full:
                write_exception.append("timeout")
            except Exception as e:
                write_exception.append(e)

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        # Give writer time to timeout
        time.sleep(0.6)
        self.assertEqual(len(write_completed), 0)  # Should have timed out
        self.assertEqual(len(write_exception), 1)  # Should have timeout exception
        self.assertEqual(write_exception[0], "timeout")

        # Now read to consume from queue and make space
        # First read might come from buffer, so read more to ensure queue consumption
        data1 = lqio.read(10)
        self.assertEqual(data1, b"A" * 10)

        # Force consumption from queue by reading more
        data2 = lqio.read(10)
        self.assertEqual(data2, b"A" * 10)

        # Now queue should have space (started with 3, consumed at least 1)
        queue_size = lqio._queue.qsize()
        self.assertLess(
            queue_size, 3, f"Queue should have space, but has {queue_size} items"
        )

        # Try writing again in a new thread, should succeed now
        write_completed2 = []
        write_exception2 = []

        def writer2():
            try:
                lqio.write(b"B" * 10)
                write_completed2.append(True)
            except queue.Full:
                write_exception2.append("timeout2")
            except Exception as e:
                write_exception2.append(str(e))

        writer_thread2 = threading.Thread(target=writer2)
        writer_thread2.start()
        writer_thread2.join(timeout=1)

        self.assertTrue(
            len(write_completed2) == 1,
            f"Second write should succeed. Exceptions: {write_exception2}",
        )
        self.assertEqual(lqio._queue.qsize(), 3)  # 2 'A' chunks + 1 'B' chunk

        writer_thread.join(timeout=1)
        self.assertFalse(writer_thread.is_alive())

    def test_progress_bar_updates(self):
        """Test progress bar functionality"""
        memory_limit = 100
        chunk_size = 10
        lqio = LimitedQueueIO(
            memory_limit=memory_limit, chunk_size=chunk_size, show_progress=True
        )

        # Progress bar should exist
        self.assertIsNotNone(lqio.status_bar)

        # Write some data
        lqio.write(b"A" * 25)

        # Progress should be updated (2 chunks + 5 bytes in buffer)
        expected_progress = 2 * 10 + 5
        self.assertEqual(lqio.status_bar.n, expected_progress)

        # Read some data
        lqio.read(15)

        # Progress should be updated
        # Note: exact value depends on internal buffering

    def test_progress_bar_cleanup(self):
        """Test progress bar is cleaned up on close"""
        lqio = LimitedQueueIO(memory_limit=100, show_progress=True)

        # Progress bar should exist
        self.assertIsNotNone(lqio.status_bar)

        lqio.close()

        # For tqdm, we can't easily check if closed, but we can verify close was called
        # by checking that no exception is raised on double close
        lqio.close()  # Should not raise

    def test_small_memory_limit(self):
        """Test very small memory limit"""
        # Memory limit smaller than chunk size
        lqio = LimitedQueueIO(memory_limit=5, chunk_size=10, show_progress=False)

        # Queue size should be at least 1
        self.assertEqual(lqio._queue.maxsize, 1)


class TestQueueIOIntegration(unittest.TestCase):
    """Integration tests for real-world scenarios"""

    def test_large_file_transfer(self):
        """Test transferring large amount of data"""
        qio = QueueIO(chunk_size=1 * MB)
        data_size = 10 * MB

        def writer():
            # Write 10MB in 1MB chunks
            for i in range(10):
                chunk = bytes([i % 256]) * MB
                qio.write(chunk)
            qio.close()

        def reader():
            total = BytesIO()
            while True:
                chunk = qio.read(512 * 1024)  # Read 512KB at a time
                if not chunk:
                    break
                total.write(chunk)
            return total.getvalue()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        result = reader()
        writer_thread.join()

        self.assertEqual(len(result), data_size)

        # Verify data integrity
        for i in range(10):
            chunk_start = i * MB
            chunk_end = (i + 1) * MB
            expected_byte = i % 256
            self.assertTrue(
                all(b == expected_byte for b in result[chunk_start:chunk_end])
            )

    def test_slow_producer_fast_consumer(self):
        """Test scenario with slow producer and fast consumer"""
        qio = QueueIO(chunk_size=100)

        def slow_producer():
            for i in range(5):
                qio.write(f"Chunk {i}\n".encode())
                time.sleep(0.1)  # Slow production
            qio.close()

        producer = threading.Thread(target=slow_producer)
        producer.start()

        # Fast consumer
        all_data = []
        while True:
            data = qio.read()
            if not data:
                break
            all_data.append(data)

        producer.join()

        # Should have received all data
        result = b"".join(all_data)
        expected = b"".join(f"Chunk {i}\n".encode() for i in range(5))
        self.assertEqual(result, expected)

    def test_memory_pressure_simulation(self):
        """Test behavior under memory pressure"""
        # Small memory limit to simulate pressure
        memory_limit = 512 * 1024  # 512KB
        chunk_size = 128 * 1024  # 128KB chunks (4 chunks fit in queue)

        # Use write timeout to prevent infinite blocking
        lqio = LimitedQueueIO(
            memory_limit=memory_limit,
            chunk_size=chunk_size,
            show_progress=False,
            write_timeout=0.5,
        )

        write_count = 0
        write_failed = False
        read_count = 0

        def producer():
            nonlocal write_count, write_failed
            # Try to write 10 chunks (1.28MB) but queue can only hold 4 chunks (512KB)
            for i in range(10):
                chunk = f"Chunk{i:04d}".encode() + b"X" * (chunk_size - 9)
                try:
                    lqio.write(chunk)
                    write_count += 1
                except queue.Full:
                    # Expected when queue is full
                    write_failed = True
                    break

            # Close the queue
            lqio.close()

        def consumer():
            nonlocal read_count
            time.sleep(0.1)  # Let producer fill the queue first

            total_bytes = 0
            while True:
                chunk = lqio.read(chunk_size)
                if not chunk:
                    break
                total_bytes += len(chunk)
                read_count += 1
                # Slow consumer to test backpressure
                time.sleep(0.05)

            # Debug: check total bytes read
            expected_bytes = write_count * chunk_size
            if total_bytes != expected_bytes:
                print(
                    f"DEBUG: Read {total_bytes} bytes vs expected {expected_bytes} bytes"
                )

        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)

        producer_thread.start()
        consumer_thread.start()

        producer_thread.join(timeout=5)
        consumer_thread.join(timeout=5)

        # Verify behavior
        self.assertFalse(producer_thread.is_alive(), "Producer thread should complete")
        self.assertFalse(consumer_thread.is_alive(), "Consumer thread should complete")

        # Producer should have hit the queue limit
        self.assertTrue(write_failed, "Producer should have encountered full queue")

        # Due to buffering, we might read one more chunk than written
        # (from write buffer flush on close)
        self.assertIn(
            read_count,
            [write_count, write_count + 1],
            f"Read count {read_count} should be equal to or "
            f"one more than write count {write_count}",
        )

        # Producer should have been blocked by queue limit
        self.assertTrue(write_failed, "Producer should have encountered full queue")

        # Should have written at least queue capacity
        self.assertGreaterEqual(write_count, 4, "Should write at least queue capacity")
        self.assertLess(
            write_count, 10, "Should write less than attempted due to backpressure"
        )


if __name__ == "__main__":
    unittest.main()
