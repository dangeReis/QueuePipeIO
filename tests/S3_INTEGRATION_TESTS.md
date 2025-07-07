# S3 Integration Tests

This directory contains comprehensive S3 integration tests for QueuePipeIO using LocalStack.

## Test Coverage

### Basic S3 Integration Tests (`test_s3_integration.py`)

1. **test_small_file_streaming** - Tests streaming files < 5MB between buckets with hash computation
2. **test_multipart_streaming** - Tests streaming large files (20MB) requiring multipart upload
3. **test_memory_pressure_streaming** - Tests streaming 50MB files with only 30MB memory limit
4. **test_concurrent_streaming** - Tests multiple concurrent streaming operations
5. **test_streaming_with_errors** - Tests error handling during streaming operations
6. **test_hash_verification** - Tests that computed hashes match expected values
7. **test_upload_download_cycle** - Tests basic upload/download using S3StreamHandler
8. **test_multipart_upload_with_handler** - Tests multipart upload through handler

### Batch S3 Integration Tests (`test_s3_batch_integration.py`)

1. **test_batch_file_copy_with_hash_verification**
   - Creates multiple files of various sizes (1MB to 25MB)
   - Uploads them to source bucket
   - Copies them to destination bucket using QueueIO
   - Computes MD5 and SHA256 hashes during transfer
   - Verifies all files copied correctly
   - Verifies all hashes match
   - Downloads and verifies content integrity

2. **test_large_file_streaming_with_progress**
   - Tests streaming a 100MB file
   - Shows progress bar during transfer
   - Computes SHA256 during streaming
   - Verifies multipart upload was used
   - Reports transfer throughput

## Key Features Tested

✅ **LocalStack Integration** - All tests use LocalStack for S3 emulation
✅ **Bucket Operations** - Create, upload, download, and delete operations
✅ **Hash Computation** - MD5 and SHA256 computed during streaming
✅ **Memory Management** - Tests with limited memory (LimitedQueueIO)
✅ **Concurrent Operations** - Multiple files transferred in parallel
✅ **Error Handling** - Tests behavior with non-existent buckets
✅ **Progress Tracking** - Visual progress bars for large transfers
✅ **Performance Metrics** - Throughput calculations for all transfers

## Running the Tests

### Prerequisites

1. Install LocalStack:
   ```bash
   pip install localstack
   ```

2. Start LocalStack:
   ```bash
   localstack start
   ```

3. Ensure AWS credentials are configured (even dummy ones):
   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   export AWS_DEFAULT_REGION=us-east-1
   ```

### Running Tests

Run all S3 integration tests:
```bash
python -m unittest tests.test_s3_integration tests.test_s3_batch_integration -v
```

Run specific test class:
```bash
python -m unittest tests.test_s3_batch_integration.TestS3BatchOperations -v
```

Run specific test method:
```bash
python -m unittest tests.test_s3_integration.TestS3Streaming.test_memory_pressure_streaming -v
```

## Performance Results

Typical performance on local machine with LocalStack:
- Small files (1-2MB): 10-20 MB/s
- Medium files (10-15MB): 25-35 MB/s  
- Large files (25-100MB): 40-60 MB/s

Performance varies based on:
- LocalStack performance
- System resources
- Chunk size configuration
- Memory limits set

## Implementation Details

The tests demonstrate proper usage of QueueIO for S3 operations:

1. **Producer-Consumer Pattern**: Download thread writes to QueueIO, upload thread reads
2. **Hash Computation**: Hashes computed during streaming without buffering entire file
3. **Memory Control**: LimitedQueueIO prevents excessive memory usage
4. **Error Handling**: Proper exception propagation between threads
5. **Progress Tracking**: Optional progress bars for user feedback