SHELL := /bin/bash
.PHONY: test test-unit test-integration test-all clean setup lint format

# Use UV for all commands
PYTHON = uv run

# Quick unit tests (no database dependencies)
test-unit:
	@echo "🧪 Running unit tests..."
	$(PYTHON) pytest tests/unit/ -m "unit" -v

# Integration tests (requires databases)
test-integration:
	@echo "🔗 Running integration tests..."
	@bash -c 'source .env.test && $(PYTHON) pytest tests/integration/ -m "integration" -v --log-cli-level=CRITICAL'

# All tests with coverage
test-all:
	@echo "🎯 Running all tests with coverage..."
	$(PYTHON) pytest --cov=src/nozzle --cov-report=html --cov-report=term-missing

# Specific loader tests
test-postgresql:
	@echo "🐘 Running PostgreSQL tests..."
	@bash -c 'source .env.test && export POSTGRES_HOST POSTGRES_PORT POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD && $(PYTHON) pytest tests/ -m "postgresql" -v --log-cli-level=ERROR'

test-redis:
	@echo "🔴 Running Redis tests..."
	@bash -c 'source .env.test && export REDIS_HOST REDIS_PORT REDIS_DB REDIS_PASSWORD && $(PYTHON) pytest tests/ -m "redis" -v --log-cli-level=ERROR'

# Performance tests
test-performance:
	@echo "⚡ Running performance tests..."
	@bash -c 'source .env.test && $(PYTHON) pytest tests/performance/ -m "performance" -v --log-cli-level=ERROR'

# Code quality (using your ruff config)
lint:
	@echo "🔍 Linting code..."
	$(PYTHON) ruff check .

format:
	@echo "✨ Formatting code..."
	$(PYTHON) ruff format .

# Setup development environment
setup:
	@echo "🚀 Setting up development environment..."
	uv sync --all-groups

# Install specific loader dependencies
install-postgresql:
	@echo "📦 Installing PostgreSQL dependencies..."
	uv sync --group postgresql

install-redis:
	@echo "📦 Installing Redis dependencies..."
	uv sync --group redis

# Database setup for testing
test-setup:
	@echo "🐳 Starting test databases..."
	docker run -d --name test-postgres \
		-e POSTGRES_DB=test_db \
		-e POSTGRES_USER=test_user \
		-e POSTGRES_PASSWORD=test_pass \
		-p 5432:5432 postgres:13
	docker run -d --name test-redis -p 6379:6379 redis:7-alpine
	@echo "⏳ Waiting for databases to start..."
	sleep 10

# Cleanup test databases
test-cleanup:
	@echo "🧹 Cleaning up test databases..."
	-docker stop test-postgres test-redis
	-docker rm test-postgres test-redis

# Full test cycle with setup/cleanup (good for CI)
test-ci: test-setup test-all test-cleanup

# Clean test artifacts
clean:
	@echo "🧹 Cleaning test artifacts..."
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Show available commands
help:
	@echo "Available commands:"
	@echo "  make setup            - Setup development environment"
	@echo "  make test-unit        - Run unit tests (fast)"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-all         - Run all tests with coverage"
	@echo "  make test-postgresql  - Run PostgreSQL tests"
	@echo "  make test-redis       - Run Redis tests"
	@echo "  make test-performance - Run performance tests"
	@echo "  make lint             - Lint code with ruff"
	@echo "  make format           - Format code with ruff"
	@echo "  make test-setup       - Start test databases"
	@echo "  make test-cleanup     - Stop test databases"
	@echo "  make clean            - Clean test artifacts"
