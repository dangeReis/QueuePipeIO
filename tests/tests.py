import threading
import unittest
import sys
import os

# Add parent directory to path for queuepipeio import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuepipeio import PipeWriter, PipeReader, QueuePipeIO

MB = 1024 * 1024


class TestPipeWriterReader(unittest.TestCase):
    """Test the new pipe-based architecture."""
    
    def test_basic_pipe_communication(self):
        """Test basic write and read through pipes."""
        writer = PipeWriter()
        reader = PipeReader()
        writer.connect(reader)
        
        test_data = b"Hello, World!"
        
        # Write and close in thread
        def write_data():
            writer.write(test_data)
            writer.close()
            
        thread = threading.Thread(target=write_data)
        thread.start()
        
        # Read data
        result = reader.read()
        thread.join()
        
        self.assertEqual(result, test_data)
        
    def test_pipe_operator(self):
        """Test pipe operator for chaining."""
        writer = PipeWriter()
        reader = PipeReader()
        
        # Chain using pipe operator
        pipeline = writer | reader
        self.assertIs(pipeline, reader)
        
        test_data = b"Pipe operator test"
        writer.write(test_data)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, test_data)
        
    def test_memory_limited_pipe(self):
        """Test memory-limited pipe operations."""
        # 1MB limit with 256KB chunks
        writer = PipeWriter(memory_limit=1*MB, chunk_size=256*1024)
        reader = PipeReader()
        writer.connect(reader)
        
        # Generate 2MB of data
        test_data = b"x" * (2 * MB)
        
        written = []
        read_data = []
        
        def write_chunks():
            # Write in 512KB chunks
            chunk_size = 512 * 1024
            for i in range(0, len(test_data), chunk_size):
                chunk = test_data[i:i+chunk_size]
                writer.write(chunk)
                written.append(len(chunk))
            writer.close()
            
        def read_chunks():
            while True:
                chunk = reader.read(128 * 1024)  # Read 128KB at a time
                if not chunk:
                    break
                read_data.append(chunk)
                
        # Start both threads
        write_thread = threading.Thread(target=write_chunks)
        read_thread = threading.Thread(target=read_chunks)
        
        write_thread.start()
        read_thread.start()
        
        write_thread.join()
        read_thread.join()
        
        # Verify data integrity
        self.assertEqual(b"".join(read_data), test_data)
        self.assertEqual(sum(written), len(test_data))
        
    def test_multiple_writes_single_read(self):
        """Test multiple write operations with single read."""
        writer = PipeWriter()
        reader = PipeReader()
        writer.connect(reader)
        
        chunks = [b"First ", b"Second ", b"Third"]
        
        for chunk in chunks:
            writer.write(chunk)
        writer.close()
        
        result = reader.read()
        self.assertEqual(result, b"".join(chunks))
        
    def test_partial_reads(self):
        """Test reading specific number of bytes."""
        writer = PipeWriter(chunk_size=10)  # Small chunks
        reader = PipeReader()
        writer.connect(reader)
        
        test_data = b"0123456789ABCDEFGHIJ"
        writer.write(test_data)
        writer.close()
        
        # Read in parts
        part1 = reader.read(5)
        part2 = reader.read(5)
        part3 = reader.read(10)
        
        self.assertEqual(part1, b"01234")
        self.assertEqual(part2, b"56789")
        self.assertEqual(part3, b"ABCDEFGHIJ")
        
    def test_writer_not_connected_error(self):
        """Test error when writer is not connected."""
        writer = PipeWriter()
        
        with self.assertRaises(RuntimeError) as cm:
            writer.write(b"test")
        self.assertIn("not connected", str(cm.exception))
        
    def test_reader_not_connected_error(self):
        """Test error when reader is not connected."""
        reader = PipeReader()
        
        with self.assertRaises(RuntimeError) as cm:
            reader.read()
        self.assertIn("not connected", str(cm.exception))
        
    def test_double_connect_error(self):
        """Test error when connecting twice."""
        writer = PipeWriter()
        reader1 = PipeReader()
        reader2 = PipeReader()
        
        writer.connect(reader1)
        
        with self.assertRaises(RuntimeError) as cm:
            writer.connect(reader2)
        self.assertIn("already connected", str(cm.exception))
        
    def test_io_on_closed_writer(self):
        """Test error when writing to closed writer."""
        writer = PipeWriter()
        reader = PipeReader()
        writer.connect(reader)
        
        writer.close()
        
        with self.assertRaises(ValueError) as cm:
            writer.write(b"test")
        self.assertIn("closed file", str(cm.exception))
        
    def test_io_on_closed_reader(self):
        """Test error when reading from closed reader."""
        writer = PipeWriter()
        reader = PipeReader()
        writer.connect(reader)
        
        reader.close()
        
        with self.assertRaises(ValueError) as cm:
            reader.read()
        self.assertIn("closed file", str(cm.exception))


class TestQueuePipeIO(unittest.TestCase):
    """Test backward compatibility wrapper."""
    
    def setUp(self):
        self.qio = QueuePipeIO()
    
    def test_write(self):
        self.assertEqual(self.qio.write(b"Hello, world!"), 13)
    
    def test_read(self):
        def write_and_close():
            self.qio.write(b"Hello, world!")
            self.qio.close()
        
        threading.Thread(target=write_and_close).start()
        self.assertEqual(self.qio.read(), b"Hello, world!")
    
    def test_close(self):
        self.qio.close()
        self.assertTrue(self.qio.closed)
        
    def test_memory_limit(self):
        """Test QueuePipeIO with memory limit."""
        qio = QueuePipeIO(memory_limit=1*MB, chunk_size=256*1024)
        
        test_data = b"Test data " * 1000
        
        def writer():
            qio.write(test_data)
            qio.close()
            
        thread = threading.Thread(target=writer)
        thread.start()
        
        result = qio.read()
        thread.join()
        
        self.assertEqual(result, test_data)


if __name__ == "__main__":
    unittest.main()