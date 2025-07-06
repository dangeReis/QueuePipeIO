# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QueuePipeIO is a Python library that provides queue-based I/O functionality for multi-threaded or asynchronous programming scenarios. It offers thread-safe data transfer between producers and consumers using Python's queue.Queue.

## Common Development Commands

### Testing
```bash
# Run all tests
python -m unittest discover -s tests

# Run specific test
python -m unittest tests.tests.TestQueueIO
```

### Building
```bash
# Install build dependencies
pip install -r requirements.txt
pip install pypandoc

# Build distributions
python -m build --sdist --wheel --outdir dist/
```

### Version Management
```bash
# Automatic version bumping (updates all version references)
bump2version patch  # 0.1.12 -> 0.1.13
bump2version minor  # 0.1.12 -> 0.2.0
bump2version major  # 0.1.12 -> 1.0.0
```

## Architecture

### Core Classes

1. **QueueIO** (`queuepipeio/__init__.py`):
   - Implements `io.RawIOBase` interface
   - Uses `queue.Queue` for thread-safe data transfer
   - Chunks data into configurable sizes (default 8MB)
   - Maintains internal buffer for partial reads/writes

2. **LimitedQueueIO** (`queuepipeio/__init__.py`):
   - Extends QueueIO with memory limit constraints
   - Adds progress bar visualization using tqdm
   - Calculates queue size based on memory_limit/chunk_size

### Design Patterns
- **Producer-Consumer**: Main use case for multi-threaded data processing
- **Chunked Processing**: Data is processed in chunks to manage memory efficiently
- **Thread Safety**: All operations are thread-safe through queue.Queue

### Key Implementation Details
- Close operation uses None sentinel value to signal EOF
- Write operations chunk data and put into queue
- Read operations maintain a buffer for partial chunk reads
- Queue polling uses time.sleep(0.01) when empty (potential optimization point)

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/publish-to-pypi.yml`):
1. Triggers on push to main branch
2. Automatically bumps patch version
3. Runs tests
4. Builds sdist and wheel distributions
5. Publishes to PyPI using API token

## Important Notes

### Naming Inconsistency
The package exports classes with different names than their definitions:
- `QueueIO` is exported as `QueuePipeIO`
- `LimitedQueueIO` is exported as `LimitedQueuePipeIO`

### Unused Dependencies
The code imports but doesn't currently use:
- boto3 (AWS SDK)
- zstandard (compression library)

### Testing Considerations
- Tests use threading to simulate producer-consumer scenarios
- No performance benchmarks or integration tests exist
- Consider adding tests for edge cases (memory limits, large files)

### Potential Improvements
- Replace `time.sleep()` polling with queue.get(timeout=...) for efficiency
- Add type hints for better code clarity
- Configure linting tools (flake8, black, mypy)
- Add performance benchmarks for different chunk sizes