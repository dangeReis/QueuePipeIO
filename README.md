# QueuePipeIO and LimitedQueuePipeIO

This Python package provides two classes, `QueuePipeIO` and `LimitedQueuePipeIO`, that represent queue-based I/O objects. These classes are ideal for multi-threaded or asynchronous programming where data is produced in one thread or coroutine and consumed in another.

## Installation

You can install this package from PyPI:

```
pip install queuepipeio
```

## Usage

Here's a basic example of how to use `QueuePipeIO` and `LimitedQueuePipeIO`:

```python
from queuepipeio import QueuePipeIO, LimitedQueuePipeIO

# Define MB as a constant
MB = 1024 * 1024

# Create a QueuePipeIO object
qpio = QueuePipeIO(chunk_size=8*MB)

# Write data to the queue
qpio.write(b'Hello, world!')

# Close the writer
qpio.close()

# Read data from the queue
data = qpio.read()

print(data)  # Outputs: b'Hello, world!'

# Create a LimitedQueuePipeIO object with a memory limit
lqpio = LimitedQueuePipeIO(memory_limit=16*MB, chunk_size=8*MB)

# Write data to the queue
lqpio.write(b'Hello, again!')

# Close the writer
lqpio.close()

# Read data from the queue
data = lqpio.read()

print(data)  # Outputs: b'Hello, again!'
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.