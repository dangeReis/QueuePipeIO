"""
QueuePipeIO - A pipe-based I/O library for Python

This module provides a pipe-based I/O system that allows for unidirectional
data flow between components, similar to Unix pipes.

Example usage:
    writer = PipeWriter()
    reader = PipeReader()
    writer.connect(reader)

    # Write in one thread
    writer.write(b"Hello, World!")
    writer.close()

    # Read in another thread
    data = reader.read()
"""

import io
import queue
import threading
import hashlib
from abc import ABC, abstractmethod

from tqdm import tqdm

MB = 1024 * 1024  # 1 MB


class PipeBase(ABC):
    """Abstract base class for pipe components."""

    @abstractmethod
    def close(self):
        """Close the pipe component."""
        pass

    @abstractmethod
    def __or__(self, other):
        """Allow pipe chaining with | operator."""
        pass


class PipeWriter(PipeBase, io.RawIOBase):
    """
    Write-only endpoint of a pipe.

    This class provides a write-only interface to put data into a queue
    that can be read by a connected PipeReader.

    Args:
        memory_limit (int, optional): Maximum memory limit in bytes.
        chunk_size (int): Size of data chunks (default 8MB).
        show_progress (bool): Whether to show progress bar.
        write_timeout (float, optional): Timeout for write operations.
    """

    def __init__(
        self,
        memory_limit=None,
        chunk_size=8 * MB,
        show_progress=False,
        write_timeout=None,
    ):
        super().__init__()
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        self._chunk_size = chunk_size
        self._write_timeout = write_timeout
        self._write_buffer = b""
        self._closed = False
        self._queue = None
        self._connected_reader = None
        self._memory_limit = memory_limit
        self.show_progress = show_progress
        self.progress_bar = None

        # Validate memory limit
        if memory_limit is not None and memory_limit <= 0:
            raise ValueError("memory_limit must be positive")

    def connect(self, reader: "PipeReader"):
        """Connect this writer to a reader."""
        if self._connected_reader is not None:
            raise RuntimeError("Writer already connected to a reader")
        if not isinstance(reader, PipeReader):
            raise TypeError("Can only connect to PipeReader")

        # Create queue on first connection
        if self._queue is None:
            if self._memory_limit is not None:
                queue_size = max(1, self._memory_limit // self._chunk_size)
                self._queue = queue.Queue(maxsize=queue_size)
            else:
                self._queue = queue.Queue()

            # Create progress bar if needed
            if self.show_progress and self._memory_limit:
                self.progress_bar = tqdm(
                    total=self._memory_limit,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=1,
                )

        self._connected_reader = reader
        reader._set_queue(self._queue)

    def write(self, b: bytes) -> int:
        """Write data to the pipe."""
        if not isinstance(b, (bytes, bytearray)):
            raise TypeError(
                f"a bytes-like object is required, not '{type(b).__name__}'"
            )

        if self._closed:
            raise ValueError("I/O operation on closed file")

        if self._queue is None:
            raise RuntimeError("Writer not connected to any reader")

        self._write_buffer += b

        # Chunk and send data
        while len(self._write_buffer) >= self._chunk_size:
            chunk = self._write_buffer[: self._chunk_size]
            self._write_buffer = self._write_buffer[self._chunk_size :]

            if self._write_timeout is None:
                self._queue.put(chunk, block=True)
            else:
                self._queue.put(chunk, block=True, timeout=self._write_timeout)

        # Update progress bar
        if self.progress_bar:
            queue_bytes = self._queue.qsize() * self._chunk_size
            buffer_bytes = len(self._write_buffer)
            self.progress_bar.n = queue_bytes + buffer_bytes
            self.progress_bar.refresh()

        return len(b)

    def close(self):
        """Close the writer and send EOF marker."""
        if self._closed:
            return

        self._closed = True

        # Flush remaining buffer
        if self._write_buffer and self._queue is not None:
            if self._write_timeout is None:
                self._queue.put(self._write_buffer, block=True)
            else:
                self._queue.put(
                    self._write_buffer, block=True, timeout=self._write_timeout
                )
            self._write_buffer = b""

        # Send EOF marker
        if self._queue is not None:
            self._queue.put(None, block=True)

        # Close progress bar
        if self.progress_bar:
            self.progress_bar.close()

    def readable(self):
        return False

    def writable(self):
        return not self._closed

    def seekable(self):
        return False

    @property
    def closed(self):
        return self._closed

    def __or__(self, other):
        """Pipe operator for chaining."""
        if isinstance(other, PipeReader):
            self.connect(other)
            return other
        elif isinstance(other, PipeFilter):
            self.connect(other.input)
            other.start()
            return other
        else:
            raise TypeError(f"Cannot pipe to {type(other)}")


class PipeReader(PipeBase, io.RawIOBase):
    """
    Read-only endpoint of a pipe.

    This class provides a read-only interface to get data from a queue
    that was written by a connected PipeWriter.
    """

    def __init__(self):
        super().__init__()
        self._queue = None
        self._buffer = b""
        self._closed = False
        self._eof = False

    def _set_queue(self, queue_obj):
        """Set the queue to read from (called by PipeWriter.connect)."""
        if self._queue is not None:
            raise RuntimeError("Reader already connected to a queue")
        self._queue = queue_obj

    def read(self, n=-1) -> bytes:
        """Read data from the pipe."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if self._queue is None:
            raise RuntimeError("Reader not connected to any writer")

        if n == 0:
            return b""

        # If we have enough buffered data, return it
        if n > 0 and len(self._buffer) >= n:
            data = self._buffer[:n]
            self._buffer = self._buffer[n:]
            return data

        # Read more data from queue
        while True:
            # Check if we have enough data now
            if n > 0 and len(self._buffer) >= n:
                data = self._buffer[:n]
                self._buffer = self._buffer[n:]
                return data

            # If EOF and no more data, return what we have
            if self._eof:
                data = self._buffer
                self._buffer = b""
                return data

            try:
                chunk = self._queue.get(timeout=0.1)
                if chunk is None:
                    # EOF marker
                    self._eof = True
                    if n < 0:
                        # Return all buffered data
                        data = self._buffer
                        self._buffer = b""
                        return data
                else:
                    self._buffer += chunk

            except queue.Empty:
                # If we have some data and n is specified, return what we have
                if n > 0 and self._buffer:
                    data = self._buffer[:n]
                    self._buffer = self._buffer[n:]
                    return data
                # Otherwise continue waiting

    def close(self):
        """Close the reader."""
        self._closed = True
        self._buffer = b""

    def readable(self):
        return not self._closed

    def writable(self):
        return False

    def seekable(self):
        return False

    @property
    def closed(self):
        return self._closed

    def __or__(self, other):
        """Pipe operator for chaining."""
        if isinstance(other, PipeWriter):
            # Create a filter that copies data
            copy_filter = CopyFilter()
            copy_filter.input._set_queue(self._queue)
            copy_filter.output = other
            copy_filter.start()
            return other
        elif isinstance(other, PipeFilter):
            other.input._set_queue(self._queue)
            other.start()
            return other
        else:
            raise TypeError(f"Cannot pipe to {type(other)}")


class PipeFilter(PipeBase):
    """
    Abstract base class for pipe filters.

    A filter reads from one pipe, processes the data, and writes to another.
    """

    def __init__(self):
        super().__init__()
        self.input = PipeReader()
        self.output = None
        self._thread = None
        self._closed = False

    @abstractmethod
    def process(self, data: bytes) -> bytes:
        """Process a chunk of data."""
        pass

    def start(self):
        """Start the filter processing thread."""
        if self._thread is None:
            self._thread = threading.Thread(target=self._run)
            self._thread.daemon = True
            self._thread.start()

    def _run(self):
        """Run the filter processing loop."""
        try:
            while not self._closed:
                chunk = self.input.read(self._chunk_size)
                if not chunk:
                    break

                processed = self.process(chunk)
                if processed and self.output:
                    self.output.write(processed)

        finally:
            if self.output:
                self.output.close()

    def close(self):
        """Close the filter."""
        self._closed = True
        if self._thread:
            self._thread.join()
        self.input.close()
        if self.output:
            self.output.close()

    def __or__(self, other):
        """Pipe operator for chaining."""
        if isinstance(other, PipeReader):
            # Connect our output writer to the reader
            if self.output is None:
                self.output = PipeWriter()
            self.output.connect(other)
            return other
        elif isinstance(other, PipeWriter):
            # Connect directly
            self.output = other
            return other
        elif isinstance(other, PipeFilter):
            # Chain filters
            if self.output is None:
                self.output = PipeWriter()
            self.output.connect(other.input)
            other.start()
            return other
        else:
            raise TypeError(f"Cannot pipe to {type(other)}")


class CopyFilter(PipeFilter):
    """Simple filter that copies data without modification."""

    def __init__(self, chunk_size=8 * MB):
        super().__init__()
        self._chunk_size = chunk_size

    def process(self, data: bytes) -> bytes:
        return data


class HashingFilter(PipeFilter):
    """
    Filter that computes hash of data passing through.

    Args:
        algorithm (str): Hash algorithm to use (default: 'sha256')
        chunk_size (int): Size of data chunks
    """

    def __init__(self, algorithm="sha256", chunk_size=8 * MB):
        super().__init__()
        self._chunk_size = chunk_size
        self._algorithm = algorithm
        self._hasher = hashlib.new(algorithm)
        self._hash_lock = threading.Lock()
        self._bytes_hashed = 0

    def process(self, data: bytes) -> bytes:
        """Update hash and pass data through."""
        with self._hash_lock:
            self._hasher.update(data)
            self._bytes_hashed += len(data)
        return data

    def get_hash(self) -> str:
        """Get the computed hash as hexdigest."""
        with self._hash_lock:
            return self._hasher.hexdigest()

    def get_bytes_hashed(self) -> int:
        """Get the number of bytes hashed."""
        with self._hash_lock:
            return self._bytes_hashed

    def reset_hash(self):
        """Reset the hash computation."""
        with self._hash_lock:
            self._hasher = hashlib.new(self._algorithm)
            self._bytes_hashed = 0


# Convenience class that combines writer and reader for backward compatibility
class QueuePipeIO:
    """
    Backward compatibility wrapper that provides both read and write.

    Note: This is provided for compatibility but the new architecture
    encourages using separate PipeWriter and PipeReader instances.
    """

    def __init__(self, memory_limit=None, chunk_size=8 * MB, show_progress=False):
        self.writer = PipeWriter(memory_limit, chunk_size, show_progress)
        self.reader = PipeReader()
        self.writer.connect(self.reader)

    def write(self, b):
        return self.writer.write(b)

    def read(self, n=-1):
        return self.reader.read(n)

    def close(self):
        self.writer.close()

    def close_all(self):
        self.writer.close()
        self.reader.close()

    @property
    def closed(self):
        return self.writer.closed

    def readable(self):
        return self.reader.readable()

    def writable(self):
        return self.writer.writable()

    def seekable(self):
        return False


# Aliases for backward compatibility
QueueIO = QueuePipeIO
LimitedQueueIO = QueuePipeIO
LimitedQueuePipeIO = QueuePipeIO

# New-style exports
__all__ = [
    "PipeWriter",
    "PipeReader",
    "PipeFilter",
    "HashingFilter",
    "CopyFilter",
    "QueuePipeIO",
    "QueueIO",
    "LimitedQueueIO",
    "LimitedQueuePipeIO",
]
