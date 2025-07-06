.PHONY: test lint fmt check clean localstack-start localstack-stop localstack-status localstack-logs test-s3

# Python command that uses venv if available, otherwise python3
PYTHON := $(shell if [ -d venv ] && [ -f venv/bin/python ]; then echo venv/bin/python; else echo python3; fi)

# LocalStack configuration
LOCALSTACK_CONTAINER := queuepipeio-localstack
AWS_ENDPOINT := http://localhost:4566

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

# LocalStack management commands
localstack-start:
	@echo "🚀 Starting LocalStack..."
	@docker-compose up -d
	@echo "⏳ Waiting for LocalStack to be ready..."
	@timeout 30 bash -c 'until docker-compose exec -T localstack curl -f http://localhost:4566/_localstack/health 2>/dev/null | grep -q "\"s3\": \"available\""; do echo -n "."; sleep 1; done' || (echo "\n❌ LocalStack failed to start" && exit 1)
	@echo "\n✅ LocalStack is ready!"

localstack-stop:
	@echo "🛑 Stopping LocalStack..."
	@docker-compose down -v
	@echo "✅ LocalStack stopped"

localstack-status:
	@echo "📊 LocalStack Status:"
	@docker-compose ps
	@echo "\n🏥 Health Check:"
	@docker-compose exec -T localstack curl -s http://localhost:4566/_localstack/health | python3 -m json.tool || echo "❌ LocalStack is not running"

localstack-logs:
	@docker-compose logs -f localstack

# Run S3-specific tests with LocalStack
test-s3: localstack-start
	@echo "🧪 Running S3 integration tests..."
	AWS_ENDPOINT_URL=$(AWS_ENDPOINT) AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test $(PYTHON) -m unittest discover -s tests -p "*s3*.py" -v
	@$(MAKE) localstack-stop