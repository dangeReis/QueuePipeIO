import io
import queue
from time import sleep

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

    Methods:
        write(b): Write data to the queue.
        read(n=-1): Read data from the queue.
        close(): Close the queue.
        seekable(): Indicate that this object is not seekable.
        readable(): Indicate that this object is readable.
        writable(): Indicate that this object is writable.
    """

    def __init__(self, chunk_size=8 * MB):
        """
        Initialize a QueueIO object.

        Args:
            chunk_size (int): Size of the chunks to put into the queue. Default is 8*MB.
        """
        super().__init__()
        self._queue = queue.Queue()  # Queue to hold the data
        self._buffer = b""  # Buffer to hold the data temporarily
        self._write_buffer = (
            b""  # Write buffer to hold the data before it's put into the queue
        )
        self._chunk_size = chunk_size  # Size of the chunks to put into the queue

    def write(self, b):
        """
        Write data to the queue.

        Args:
            b (bytes): The data to be written.

        Returns:
            int: The number of bytes written.
        """
        self._write_buffer += b
        while len(self._write_buffer) >= self._chunk_size:
            chunk, self._write_buffer = (
                self._write_buffer[: self._chunk_size],
                self._write_buffer[self._chunk_size :],
            )
            self._queue.put(chunk)
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
        while len(self._buffer) < n or n == -1:
            try:
                data = self._queue.get_nowait()
                if data is None:  # If data is None, return the buffer
                    return self._buffer
                else:
                    self._buffer += data  # Add data to the buffer
            except queue.Empty:
                if self.closed:
                    if self._buffer != b"":
                        data = self._buffer
                        self._buffer = b""
                        return data
                    else:
                        break
                else:
                    sleep(0.1)  # Sleep for a short time if the queue is empty

        # Split the buffer into the data to return and the remaining buffer
        if n == -1:
            data, self._buffer = self._buffer, b""
        else:
            data, self._buffer = self._buffer[:n], self._buffer[n:]

        return data

    def close(self):
        """Close the queue"""
        if len(self._write_buffer) > 0:  # If there's remaining data in the write buffer
            self._queue.put(self._write_buffer)  # Put the remaining data into the queue
            self._write_buffer = b""  # Clear the write buffer
        self._queue.put(None)  # Put None in the queue to signal that it's closed
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

    Attributes:
        _queue (Queue): The queue used to store the chunks of data.
        _buffer (bytes): The buffer used to store the remaining data.
        status_bar (tqdm.tqdm): The progress bar used to track the memory usage.

    Methods:
        write(b): Writes the given bytes to the stream.
        read(n=-1): Reads at most n bytes from the stream.

    """

    def __init__(self, memory_limit=None, chunk_size=8 * MB):
        """
        Initialize the QueueBytesIO object.

        Args:
            memory_limit (int, optional): The maximum memory limit in bytes. Defaults to None.
            chunk_size (int, optional): The size of each chunk in bytes. Defaults to 8*MB.
        """
        if memory_limit is not None:
            queue_size = memory_limit // chunk_size
        else:
            return super().__init__(chunk_size)
        super().__init__(chunk_size)
        self._queue = queue.Queue(maxsize=queue_size)
        self._buffer = b""
        self.status_bar = tqdm(
            total=memory_limit, unit="B", unit_scale=True, unit_divisor=1024, position=1
        )

    def write(self, b):
        """
        Writes the given bytes to the stream.

        Args:
            b (bytes): The bytes to be written.

        Returns:
            int: The number of bytes written.

        """
        # update status bar
        self.status_bar.n = self._queue.qsize() * self._chunk_size + len(self._buffer)
        self.status_bar.refresh()
        return super().write(b)

    def read(self, n=-1):
        """
        Reads at most n bytes from the stream.

        Args:
            n (int, optional): The maximum number of bytes to read.
                Defaults to -1, which means read all.

        Returns:
            bytes: The bytes read from the stream.

        """
        # update status bar
        # self.status_bar.n = self._queue.qsize() * self._chunk_size + len(self._buffer)
        # self.status_bar.refresh()
        return super().read(n)
