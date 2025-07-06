.PHONY: test lint fmt check clean

# Run all tests
test:
	python -m unittest discover -s tests

# Run linting checks
lint:
	python -m flake8 queuepipeio/ tests/ setup.py

# Format code using black
fmt:
	python -m black queuepipeio/ tests/ setup.py

# Run all checks (format check, lint, test)
check: fmt-check lint test
	@echo "✅ All checks passed!"

# Check formatting without modifying files
fmt-check:
	python -m black --check queuepipeio/ tests/ setup.py

# Clean up cache and build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf build/ dist/ *.egg-info