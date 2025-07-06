import io
import queue
import threading

from tqdm import tqdm

progress_bar = None

MB = 1024 * 1024  # 1 MB


class QueueIO(io.RawIOBase):
    """
    A class that represents a queue-based I/O object.

    This class provides a queue-based I/O functionality, where data can be written to the queue
    and read from the queue. The data is stored in chunks of a specified size.

    Attributes:
        _queue (queue.Queue): The queue to hold the data.
        _buffer (bytes): The buffer to hold the data temporarily.
        _write_buffer (bytes): The write buffer to hold the data before it's put into the queue.
        _chunk_size (int): The size of the chunks to put into the queue.
        _write_timeout (float): Timeout in seconds for write operations.

    Methods:
        write(b): Write data to the queue.
        read(n=-1): Read data from the queue.
        close(): Close the queue.
        seekable(): Indicate that this object is not seekable.
        readable(): Indicate that this object is readable.
        writable(): Indicate that this object is writable.
    """

    def __init__(self, chunk_size=8 * MB, write_timeout=None):
        """
        Initialize a QueueIO object.

        Args:
            chunk_size (int): Size of the chunks to put into the queue. Default is 8*MB.
            write_timeout (float, optional): Timeout in seconds for write operations.
                                           None means block forever (default).
        """
        super().__init__()
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._queue = queue.Queue()  # Queue to hold the data
        self._buffer = b""  # Buffer to hold the data temporarily
        self._write_buffer = (
            b""  # Write buffer to hold the data before it's put into the queue
        )
        self._chunk_size = chunk_size  # Size of the chunks to put into the queue
        self._write_timeout = write_timeout  # Timeout for write operations
        self._lock = threading.Lock()  # Lock for thread-safe buffer operations
        self._closed = False  # Track closed state
        self._close_lock = threading.Lock()  # Lock for close operations

    def write(self, b):
        """
        Write data to the queue.

        Args:
            b (bytes): The data to be written.

        Returns:
            int: The number of bytes written.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not isinstance(b, (bytes, bytearray)):
            raise TypeError(
                f"a bytes-like object is required, not '{type(b).__name__}'"
            )

        with self._lock:
            self._write_buffer += b
            while len(self._write_buffer) >= self._chunk_size:
                chunk, self._write_buffer = (
                    self._write_buffer[: self._chunk_size],
                    self._write_buffer[self._chunk_size :],
                )
                try:
                    if self._write_timeout is None:
                        self._queue.put(chunk, block=True)
                    else:
                        self._queue.put(chunk, block=True, timeout=self._write_timeout)
                except queue.Full:
                    # Re-add chunk to buffer and raise
                    self._write_buffer = chunk + self._write_buffer
                    raise
        return len(b)

    def read(self, n=-1):
        """
        Read data from the queue.

        Args:
            n (int, optional): The number of bytes to read. Defaults to -1,
                which means read all available data.

        Returns:
            bytes: The data read from the queue.
        """
        if n == 0:
            return b""

        with self._lock:
            # If we have enough data in buffer, return it immediately
            if n != -1 and len(self._buffer) >= n:
                data, self._buffer = self._buffer[:n], self._buffer[n:]
                return data

        while True:
            with self._lock:
                # Check if we have enough data now
                if n != -1 and len(self._buffer) >= n:
                    data, self._buffer = self._buffer[:n], self._buffer[n:]
                    return data

                # Check if we should return what we have
                if self._closed and self._queue.empty():
                    data = self._buffer
                    self._buffer = b""
                    return data

            try:
                # Use timeout instead of polling with sleep
                data = self._queue.get(timeout=0.1)
                if data is None:  # EOF marker
                    with self._lock:
                        result = self._buffer
                        self._buffer = b""
                        self._closed = True
                    return result
                else:
                    with self._lock:
                        self._buffer += data
            except queue.Empty:
                # Check if closed while waiting
                if self._closed:
                    with self._lock:
                        if self._buffer:
                            data = self._buffer
                            self._buffer = b""
                            return data
                        else:
                            return b""
                # Continue waiting if not closed

    def close(self):
        """Close the queue"""
        with self._close_lock:
            if self._closed:
                return  # Already closed

            with self._lock:
                if (
                    len(self._write_buffer) > 0
                ):  # If there's remaining data in the write buffer
                    try:
                        if self._write_timeout is None:
                            self._queue.put(self._write_buffer)
                        else:
                            self._queue.put(
                                self._write_buffer,
                                block=True,
                                timeout=self._write_timeout,
                            )
                    except queue.Full:
                        # If we can't flush on close due to full queue, at least try to signal EOF
                        pass
                    self._write_buffer = b""  # Clear the write buffer

            # Always try to put EOF marker, but don't block forever
            try:
                if self._write_timeout is None:
                    self._queue.put(None)
                else:
                    self._queue.put(None, block=True, timeout=self._write_timeout)
            except queue.Full:
                # If we can't put EOF marker, readers might hang
                pass
            self._closed = True
        super().close()

    def seekable(self):
        """Indicate that this object is not seekable"""
        return False

    def readable(self):
        """Indicate that this object is readable"""
        return True

    def writable(self):
        """Indicate that this object is writable"""
        return True


class LimitedQueueIO(QueueIO):
    """
    A class that represents a limited queue-based input/output stream.

    This class inherits from the `QueueIO` class and adds functionality to limit the memory usage
    by using a queue with a specified memory limit and chunk size.

    Args:
        memory_limit (int, optional): The maximum memory limit in bytes.
            If not provided, there is no memory limit.
        chunk_size (int, optional): The size of each chunk in bytes. Defaults to 8 * MB.
        show_progress (bool, optional): Whether to show a progress bar. Defaults to False.

    Attributes:
        _queue (Queue): The queue used to store the chunks of data.
        _buffer (bytes): The buffer used to store the remaining data.
        status_bar (tqdm.tqdm): The progress bar used to track the memory usage (if enabled).

    Methods:
        write(b): Writes the given bytes to the stream.
        read(n=-1): Reads at most n bytes from the stream.

    """

    def __init__(
        self,
        memory_limit=None,
        chunk_size=8 * MB,
        show_progress=False,
        write_timeout=None,
    ):
        """
        Initialize the QueueBytesIO object.

        Args:
            memory_limit (int, optional): The maximum memory limit in bytes. Defaults to None.
            chunk_size (int, optional): The size of each chunk in bytes. Defaults to 8*MB.
            show_progress (bool, optional): Whether to show a progress bar. Defaults to False.
            write_timeout (float, optional): Timeout in seconds for write operations
                when queue is full. None means block forever (default).
        """
        # Initialize parent with write_timeout
        super().__init__(chunk_size, write_timeout)
        self.show_progress = show_progress

        if memory_limit is not None:
            if memory_limit <= 0:
                raise ValueError("memory_limit must be positive")
            queue_size = max(1, memory_limit // chunk_size)
            self._queue = queue.Queue(maxsize=queue_size)
            if self.show_progress:
                self.status_bar = tqdm(
                    total=memory_limit,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    position=1,
                )
            else:
                self.status_bar = None
        else:
            self.status_bar = None

    def write(self, b):
        """
        Writes the given bytes to the stream.

        Args:
            b (bytes): The bytes to be written.

        Returns:
            int: The number of bytes written.

        """
        result = super().write(b)
        # update status bar after write
        if self.status_bar is not None:
            with self._lock:
                self.status_bar.n = self._queue.qsize() * self._chunk_size + len(
                    self._write_buffer
                )
                self.status_bar.refresh()
        return result

    def read(self, n=-1):
        """
        Reads at most n bytes from the stream.

        Args:
            n (int, optional): The maximum number of bytes to read.
                Defaults to -1, which means read all.

        Returns:
            bytes: The bytes read from the stream.

        """
        result = super().read(n)
        # update status bar
        if self.status_bar is not None:
            with self._lock:
                self.status_bar.n = self._queue.qsize() * self._chunk_size + len(
                    self._buffer
                )
                self.status_bar.refresh()
        return result

    def close(self):
        """Close the queue and cleanup resources"""
        super().close()
        if hasattr(self, "status_bar") and self.status_bar is not None:
            self.status_bar.close()
