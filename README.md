# QueueIO and LimitedQueueIO

This Python package provides two classes, `QueueIO` and `LimitedQueueIO`, that represent queue-based I/O objects. These classes are ideal for multi-threaded or asynchronous programming where data is produced in one thread or coroutine and consumed in another.

## Installation

You can install this package from PyPI:

```
pip install queue_bytes_io
```

## Usage

Here's a basic example of how to use `QueueIO` and `LimitedQueueIO`:

```python
from queuebytesio import QueueIO, LimitedQueueIO

# Define MB as a constant
MB = 1024 * 1024

# Create a QueueIO object
qbio = QueueIO(chunk_size=8*MB)

# Write data to the queue
qbio.write(b'Hello, world!')

# Close the writer
qbio.close()

# Read data from the queue
data = qbio.read()

print(data)  # Outputs: b'Hello, world!'

# Create a LimitedQueueIO object with a memory limit
lqbio = LimitedQueueIO(memory_limit=16*MB, chunk_size=8*MB)

# Write data to the queue
lqbio.write(b'Hello, again!')

# Close the writer
lqbio.close()

# Read data from the queue
data = lqbio.read()

print(data)  # Outputs: b'Hello, again!'
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.