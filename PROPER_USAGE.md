# Proper QueuePipeIO Usage Pattern

## The New Pipe Architecture

QueuePipeIO now uses a unidirectional pipe architecture where:

1. **PipeWriter** → writes to → **Queue** → read by → **PipeReader**
2. **Filters** can be inserted in the pipeline for data transformation

The key insight is that pipes provide clear, unidirectional data flow:
- Stream data without buffering the entire file
- Transform data as it flows (e.g., compute hashes)
- Control memory usage with built-in limits
- Handle backpressure automatically
- No deadlocks due to unidirectional flow

## Basic Usage

```python
from queuepipeio import PipeWriter, PipeReader

# Create and connect pipes
writer = PipeWriter()
reader = PipeReader()
writer | reader  # Connect with pipe operator

# Producer thread
def producer():
    writer.write(b"Hello, World!")
    writer.close()

# Consumer thread  
def consumer():
    data = reader.read()
    print(data)
```

## Pipeline with Filters

```python
from queuepipeio import PipeWriter, PipeReader, HashingFilter

# Create pipeline: writer -> hasher -> reader
writer = PipeWriter(memory_limit=50*1024*1024)  # 50MB limit
hasher = HashingFilter(algorithm='sha256')
reader = PipeReader()

# Chain components
writer | hasher | reader

# Data flows through the pipeline
writer.write(data)
writer.close()

# Get results
result = reader.read()
file_hash = hasher.get_hash()
```

## Real-World Example: S3 Transfer with Verification

```python
import threading
from queuepipeio import PipeWriter, PipeReader, HashingFilter

def s3_transfer_with_hash(s3_client, source_bucket, source_key, 
                         dest_bucket, dest_key):
    # Create pipeline
    writer = PipeWriter(memory_limit=100*1024*1024)  # 100MB
    hasher = HashingFilter()
    reader = PipeReader()
    
    writer | hasher | reader
    
    def download():
        """Download from S3 to pipe"""
        obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        for chunk in obj['Body'].iter_chunks(chunk_size=1024*1024):
            writer.write(chunk)
        writer.close()
    
    def upload():
        """Read from pipe and upload to S3"""
        # Multipart upload
        upload_id = s3_client.create_multipart_upload(
            Bucket=dest_bucket, Key=dest_key
        )['UploadId']
        
        parts = []
        part_num = 1
        
        while True:
            chunk = reader.read(5*1024*1024)  # 5MB parts
            if not chunk:
                break
                
            resp = s3_client.upload_part(
                Bucket=dest_bucket,
                Key=dest_key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=chunk
            )
            
            parts.append({
                'ETag': resp['ETag'],
                'PartNumber': part_num
            })
            part_num += 1
        
        # Complete upload
        s3_client.complete_multipart_upload(
            Bucket=dest_bucket,
            Key=dest_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    
    # Run transfer
    dl_thread = threading.Thread(target=download)
    ul_thread = threading.Thread(target=upload)
    
    dl_thread.start()
    ul_thread.start()
    
    dl_thread.join()
    ul_thread.join()
    
    return hasher.get_hash()
```

## Memory Management

```python
# Limit memory usage
writer = PipeWriter(
    memory_limit=10*1024*1024,  # 10MB total
    chunk_size=1*1024*1024      # 1MB chunks
)

# With 10MB limit and 1MB chunks:
# - Queue can hold max 10 chunks
# - Writer blocks when queue is full
# - Provides automatic backpressure
```

## Creating Custom Filters

```python
from queuepipeio import PipeFilter

class CompressionFilter(PipeFilter):
    """Example: Compress data as it flows through"""
    
    def __init__(self, level=6):
        super().__init__()
        self._chunk_size = 1024 * 1024
        import zlib
        self._compressor = zlib.compressobj(level)
    
    def process(self, data: bytes) -> bytes:
        """Compress chunk of data"""
        return self._compressor.compress(data)
    
    def close(self):
        """Flush remaining compressed data"""
        if self._compressor and self.output:
            final_data = self._compressor.flush()
            if final_data:
                self.output.write(final_data)
        super().close()

# Usage
writer = PipeWriter()
compressor = CompressionFilter(level=9)
reader = PipeReader()

writer | compressor | reader
```

## Best Practices

1. **Always close writers** - This sends EOF to readers
2. **Use memory limits** - Prevent unbounded memory usage
3. **Handle errors** - Wrap operations in try/except
4. **Use filters** - Keep data transformation separate from I/O
5. **Think in pipelines** - Design data flow as a series of transformations

## Migration from Old API

```python
# Old way (bidirectional, complex)
qio = QueueIO()
# Had to manage both read/write state

# New way (unidirectional, simple)  
writer = PipeWriter()
reader = PipeReader()
writer | reader
# Clear separation of concerns
```