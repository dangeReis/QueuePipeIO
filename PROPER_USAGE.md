# Proper QueueIO Usage Pattern

## The Correct Pattern

QueueIO should be used as a streaming pipeline where:

1. **Writer** (e.g., S3 download) → writes to → **QueueIO** → read by → **Reader** (e.g., S3 upload)

The key insight is that QueueIO sits in the middle of the data flow, allowing you to:
- Stream data without buffering the entire file
- Compute hashes/checksums as data flows through (either during write OR read)
- Control memory usage with LimitedQueueIO
- Handle backpressure automatically

## Hash Computation Options

### Write-Side Hashing (Producer computes hash)
Best when:
- Hash is computed once and used by multiple consumers
- Producer needs to log/verify the hash
- All consumers need the same hash algorithm

### Read-Side Hashing (Consumer computes hash) - NEW!
Best when:
- Different consumers need different hash algorithms
- Producer shouldn't know about hashing requirements  
- You want to verify data integrity on the consumer side
- Hash computation should be decoupled from data production

## Example: Write-Side Hashing (Producer computes hash)

```python
from queuepipeio import LimitedQueueIO
import hashlib
import threading

class HashingQueueIO(LimitedQueueIO):
    """QueueIO that computes hash of data passing through during write"""
    
    def __init__(self, hash_algorithm='sha256', **kwargs):
        super().__init__(**kwargs)
        self._hasher = hashlib.new(hash_algorithm)
        self._hash_lock = threading.Lock()
        
    def write(self, b):
        """Write data and update hash"""
        with self._hash_lock:
            self._hasher.update(b)
        return super().write(b)
    
    def get_hash(self):
        """Get the computed hash"""
        with self._hash_lock:
            return self._hasher.hexdigest()

# Create the hashing queue with memory limit
hashing_queue = HashingQueueIO(
    memory_limit=10 * 1024 * 1024,  # 10MB memory limit
    chunk_size=2 * 1024 * 1024,     # 2MB chunks
)

def download_from_s3():
    """S3 download writes to the queue"""
    response = s3_client.get_object(Bucket='source-bucket', Key='file.bin')
    for chunk in response['Body'].iter_chunks(chunk_size=1024*1024):
        hashing_queue.write(chunk)  # Data flows through, hash computed here
    hashing_queue.close()  # Signal EOF

def upload_to_s3():
    """S3 upload reads from the queue"""
    s3_client.upload_fileobj(
        hashing_queue,  # Read directly from queue
        'dest-bucket',
        'file-copy.bin'
    )

# Run both in parallel
download_thread = threading.Thread(target=download_from_s3)
upload_thread = threading.Thread(target=upload_to_s3)

download_thread.start()
upload_thread.start()

download_thread.join()
upload_thread.join()

# Hash is now available!
print(f"File hash: {hashing_queue.get_hash()}")
```

## Example: Read-Side Hashing (Consumer computes hash) - NEW!

```python
from queuepipeio import HashingLimitedQueueIO
import threading

# Built-in class that computes hash during read operations
hashing_queue = HashingLimitedQueueIO(
    hash_algorithm='sha256',         # Choose your algorithm  
    memory_limit=10 * 1024 * 1024,   # 10MB memory limit
    chunk_size=2 * 1024 * 1024,      # 2MB chunks
)

def download_from_s3():
    """S3 download just writes data - no hash computation"""
    response = s3_client.get_object(Bucket='source-bucket', Key='file.bin')
    for chunk in response['Body'].iter_chunks(chunk_size=1024*1024):
        hashing_queue.write(chunk)  # Just write, no hashing here
    hashing_queue.close()

def verify_and_save():
    """Consumer reads data and hash is computed automatically"""
    with open('output.bin', 'wb') as f:
        while True:
            chunk = hashing_queue.read(512*1024)  # Hash computed here!
            if not chunk:
                break
            f.write(chunk)
    
    # Get the hash computed during reading
    computed_hash = hashing_queue.get_hash()
    print(f"File hash (computed during read): {computed_hash}")
    
    # Verify against expected hash
    expected_hash = "abc123..."  # From S3 metadata
    if computed_hash == expected_hash:
        print("Integrity verified!")

# Run both in parallel
threading.Thread(target=download_from_s3).start()
threading.Thread(target=verify_and_save).start()
```

## Why This Pattern?

### ✅ Correct: Data flows through QueueIO
- Download → QueueIO (computes hash) → Upload
- Hash computation happens during streaming
- No need to read the file twice
- Memory usage is controlled

### ❌ Incorrect: Using QueueIO just as a buffer
- Download → Buffer → Compute Hash → Upload
- Requires reading data multiple times
- Defeats the purpose of streaming

## Key Benefits

1. **Single Pass**: Hash is computed as data streams through, no need to read twice
2. **Memory Efficient**: LimitedQueueIO ensures memory usage stays within bounds
3. **Concurrent**: Download and upload happen simultaneously
4. **Backpressure**: If upload is slow, download automatically slows down
5. **Thread Safe**: Multiple threads can safely read/write

## Common Use Cases

### 1. S3 to S3 Transfer with Verification
```python
# As shown above - compute hash during transfer
```

### 2. HTTP Download to S3 with Progress
```python
class ProgressHashingQueueIO(HashingQueueIO):
    def __init__(self, total_size, **kwargs):
        super().__init__(**kwargs)
        self.progress = tqdm(total=total_size, unit='B', unit_scale=True)
        
    def write(self, b):
        result = super().write(b)
        self.progress.update(len(b))
        return result

# Use for downloads with progress bar
queue = ProgressHashingQueueIO(
    total_size=file_size,
    memory_limit=50*1024*1024
)
```

### 3. Multi-Stream Processing
```python
# Process multiple files concurrently
queues = []
for file in files:
    q = HashingQueueIO(memory_limit=10*1024*1024)
    queues.append(q)
    
    # Start download/upload threads for each queue
    threading.Thread(target=download_file, args=(file, q)).start()
    threading.Thread(target=upload_file, args=(file, q)).start()

# All files process concurrently with controlled memory usage
```

## Best Practices

1. **Always close() the write side** to signal EOF to readers
2. **Use appropriate chunk sizes** - typically 1-10MB for cloud storage
3. **Set memory limits** based on available system memory
4. **Handle exceptions** in both reader and writer threads
5. **Add monitoring** - progress bars, throughput metrics, etc.

## Integration with Cloud Storage

### AWS S3
```python
# Works directly with boto3
s3.upload_fileobj(queue_io, bucket, key)
s3.download_fileobj(bucket, key, queue_io)
```

### Google Cloud Storage
```python
# Works with google-cloud-storage
blob.upload_from_file(queue_io)
blob.download_to_file(queue_io)
```

### Azure Blob Storage
```python
# Works with azure-storage-blob
blob_client.upload_blob(queue_io)
blob_client.download_blob().readinto(queue_io)
```

## Debugging Tips

1. **Check queue size**: `queue_io._queue.qsize()` shows items waiting
2. **Monitor memory**: `queue_io._queue.maxsize * queue_io._chunk_size` shows max memory
3. **Verify hash**: Always compare computed hash with expected
4. **Log progress**: Add logging to track bytes written/read
5. **Test backpressure**: Artificially slow down reader to test behavior