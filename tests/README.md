# QueuePipeIO Tests

This directory contains the test suite for QueuePipeIO, including unit tests and S3 integration tests.

## Running Tests

### Basic Tests (No S3)
```bash
python -m unittest discover -s tests -v
```

### S3 Integration Tests
S3 tests automatically start LocalStack if it's not running. The tests will:
1. Check if LocalStack is running
2. Start it if needed
3. Run the S3 tests
4. Stop LocalStack when done (unless KEEP_LOCALSTACK is set)

You have several options:

#### Option 1: Automatic LocalStack Management (Default)
```bash
# Just run the tests - LocalStack starts automatically
python -m unittest tests.test_s3_integration -v

# Keep LocalStack running between test runs (faster)
KEEP_LOCALSTACK=true python -m unittest tests.test_s3_integration -v

# Run all tests (LocalStack auto-starts for S3 tests)
python -m unittest discover -s tests -v
```

#### Option 2: Manual LocalStack Management
```bash
# Start LocalStack
make localstack-start

# Run tests
python -m unittest tests.test_s3_integration -v

# Stop LocalStack
make localstack-stop
```

#### Option 3: Use Make Commands
```bash
# Runs S3 tests (starts LocalStack automatically)
make test-s3
```

## VSCode Integration

### Running Tests in VSCode

**S3 tests now start LocalStack automatically!** Just run tests normally in VSCode:

1. **Using Test Explorer** (Recommended):
   - Click the test you want to run
   - LocalStack will start automatically for S3 tests
   - Set `KEEP_LOCALSTACK=true` in `.env` to keep it running between tests

2. **Using Tasks**:
   - Press `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
   - Type "Tasks: Run Task"
   - Select one of:
     - "Run All Tests (with S3)" - Runs all tests
     - "Run S3 Tests" - Runs only S3 tests
     - "Start LocalStack" - Manually start LocalStack
     - "Stop LocalStack" - Manually stop LocalStack

3. **Environment Variables**:
   Create a `.env` file in the project root:
   ```bash
   # Keep LocalStack running between test runs
   KEEP_LOCALSTACK=true
   ```

### Keyboard Shortcuts
You can add keyboard shortcuts for common test tasks in your `keybindings.json`:

```json
[
    {
        "key": "cmd+shift+t",
        "command": "workbench.action.tasks.runTask",
        "args": "Run All Tests (with S3)"
    }
]
```

## Test Structure

- `test_pipe_architecture.py` - Comprehensive unit tests for the pipe architecture
- `test_pipe_filters.py` - Tests for filters (HashingFilter, etc.)
- `test_s3_integration.py` - S3 integration tests using LocalStack
- `tests.py` - Basic compatibility tests

## Writing New Tests

### Unit Tests
```python
class TestNewFeature(unittest.TestCase):
    def test_feature(self):
        writer = PipeWriter()
        reader = PipeReader()
        writer | reader
        # ... test logic
```

### S3 Integration Tests
```python
@skipUnless(BOTO3_AVAILABLE and localstack_available(), 
            "LocalStack not available")
class TestS3Feature(unittest.TestCase):
    def test_s3_operation(self):
        # ... test logic
```

## Troubleshooting

### LocalStack Issues
- Check if Docker is running
- Verify LocalStack health: `curl http://localhost:4566/_localstack/health`
- Check logs: `make localstack-logs`
- Reset LocalStack: `make localstack-stop && make localstack-start`

### Test Discovery Issues
- Ensure test files start with `test_`
- Check Python path includes project root
- Verify virtual environment is activated

### S3 Test Failures
- Ensure test bucket exists: `python tests/test_localstack_setup.py`
- Check LocalStack S3 endpoint: `http://localhost:4566`
- Verify AWS credentials (should be "test"/"test" for LocalStack)