.PHONY: test lint fmt check clean

# Python command that uses venv if available, otherwise python3
PYTHON := $(shell if [ -d venv ] && [ -f venv/bin/python ]; then echo venv/bin/python; else echo python3; fi)

# Run all tests
test:
	$(PYTHON) -m unittest discover -s tests

# Run linting checks
lint:
	$(PYTHON) -m flake8 queuepipeio/ tests/ setup.py

# Format code using black
fmt:
	$(PYTHON) -m black queuepipeio/ tests/ setup.py

# Run all checks (format check, lint, test)
check: fmt-check lint test
	@echo "✅ All checks passed!"

# Check formatting without modifying files
fmt-check:
	$(PYTHON) -m black --check queuepipeio/ tests/ setup.py

# Clean up cache and build artifacts
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf build/ dist/ *.egg-info

# Install dependencies in venv
install:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -r test-requirements.txt
	$(PYTHON) -m pip install flake8 black