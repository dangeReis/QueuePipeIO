# QueuePipeIO

A Python library that provides a pipe-based I/O system for efficient data transfer between threads, with support for data transformation filters and memory management.

## Features

- **Unidirectional Pipes**: Separate `PipeWriter` and `PipeReader` classes for clear data flow
- **Pipe Operator**: Chain components using the `|` operator (like Unix pipes)
- **Memory Management**: Built-in backpressure with configurable memory limits
- **Data Filters**: Transform data as it flows through the pipeline (e.g., hash computation)
- **Thread-Safe**: Designed for multi-threaded producer/consumer patterns
- **S3 Integration**: Optimized for streaming large files to/from S3 with integrity verification

## Installation

```bash
pip install queuepipeio
```

## Quick Start

### Basic Usage

```python
from queuepipeio import PipeWriter, PipeReader

# Create a pipe
writer = PipeWriter()
reader = PipeReader()
writer | reader  # Connect with pipe operator

# In producer thread
writer.write(b"Hello, World!")
writer.close()

# In consumer thread
data = reader.read()
print(data)  # b"Hello, World!"
```

### With Memory Limit

```python
from queuepipeio import PipeWriter, PipeReader

MB = 1024 * 1024

# Limit memory usage to 10MB
writer = PipeWriter(memory_limit=10*MB, chunk_size=1*MB)
reader = PipeReader()
writer | reader

# Memory limit provides backpressure if consumer is slower
# Writer will block when queue is full
```

### Pipeline with Hash Computation

```python
from queuepipeio import PipeWriter, PipeReader, HashingFilter

# Create a pipeline: writer -> hasher -> reader
writer = PipeWriter()
hasher = HashingFilter(algorithm='sha256')
reader = PipeReader()

# Chain components
writer | hasher | reader

# Write data
writer.write(b"Important data")
writer.close()

# Read data (unchanged)
data = reader.read()

# Get computed hash
file_hash = hasher.get_hash()
print(f"SHA256: {file_hash}")
```

## Real-World Example: S3 Streaming with Verification

```python
import boto3
import threading
from queuepipeio import PipeWriter, PipeReader, HashingFilter

def stream_s3_download(s3_client, bucket, key, local_file):
    """Download from S3 with hash verification"""
    
    # Create pipeline
    writer = PipeWriter(memory_limit=50*1024*1024)  # 50MB limit
    hasher = HashingFilter('sha256')
    reader = PipeReader()
    
    writer | hasher | reader
    
    def download_thread():
        """Download from S3 to pipe"""
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        for chunk in response['Body'].iter_chunks(chunk_size=1024*1024):
            writer.write(chunk)
        
        writer.close()
    
    def save_thread():
        """Read from pipe and save to file"""
        with open(local_file, 'wb') as f:
            while True:
                chunk = reader.read(1024*1024)
                if not chunk:
                    break
                f.write(chunk)
    
    # Start threads
    dl_thread = threading.Thread(target=download_thread)
    save_thread = threading.Thread(target=save_thread)
    
    dl_thread.start()
    save_thread.start()
    
    dl_thread.join()
    save_thread.join()
    
    # Return computed hash for verification
    return hasher.get_hash()
```

## Architecture

### Core Components

- **PipeWriter**: Write-only endpoint that puts data into a queue
- **PipeReader**: Read-only endpoint that reads data from a queue  
- **PipeFilter**: Base class for data transformation filters
- **HashingFilter**: Computes hash of data passing through

### Key Benefits

- **No Deadlocks**: Unidirectional flow eliminates race conditions
- **Composable**: Easy to chain components and add filters
- **Memory Efficient**: Automatic backpressure prevents memory overflow
- **High Performance**: Minimal overhead, suitable for large file transfers

## Testing

```bash
# Run all tests
python -m unittest discover -s tests

# Run with S3 integration tests (requires Docker)
# S3 tests automatically start LocalStack if needed
python -m unittest tests.test_s3_integration -v

# Keep LocalStack running between test runs
KEEP_LOCALSTACK=true python -m unittest discover -s tests
```

## Migration from Old API

If you're using the old `QueueIO` class:

```python
# Old way
from queuepipeio import QueueIO
qio = QueueIO()
qio.write(data)
result = qio.read()

# New way  
from queuepipeio import PipeWriter, PipeReader
writer = PipeWriter()
reader = PipeReader()
writer | reader
writer.write(data)
writer.close()
result = reader.read()
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.