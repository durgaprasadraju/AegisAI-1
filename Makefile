.PHONY: help build test lint clean setup deploy proto

# Default target
help:
	@echo "AegisAI Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build      - Build all services"
	@echo "  make test       - Run all tests"
	@echo "  make lint       - Run linters"
	@echo "  make clean      - Clean build artifacts"
	@echo "  make setup      - Setup local development environment"
	@echo "  make deploy     - Deploy services (usage: make deploy ENV=dev)"
	@echo "  make proto      - Generate protocol buffer code"
	@echo "  make docker-up  - Start local services with Docker Compose"
	@echo "  make docker-down - Stop local services"

# Build all services
build:
	@./scripts/build.sh

# Run tests
test:
	@echo "Running tests..."
	@go test ./...

# Run linters
lint:
	@echo "Running linters..."
	@golangci-lint run

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -rf dist/
	@rm -rf build/
	@find . -name "*.test" -delete
	@find . -name "*.out" -delete

# Setup local environment
setup:
	@./scripts/setup-local.sh

# Deploy services
deploy:
	@if [ -z "$(ENV)" ]; then \
		echo "Error: ENV is required. Usage: make deploy ENV=dev"; \
		exit 1; \
	fi
	@./scripts/deploy.sh $(ENV)

# Generate protocol buffers
proto:
	@./scripts/generate-proto.sh

# Docker Compose commands
docker-up:
	@cd docker && docker-compose up -d

docker-down:
	@cd docker && docker-compose down

docker-logs:
	@cd docker && docker-compose logs -f

# Service-specific targets
build-data-generator:
	@./scripts/build.sh data-generator

build-ingestion:
	@./scripts/build.sh ingestion-service

build-ml:
	@./scripts/build.sh ml-service

build-alert:
	@./scripts/build.sh alert-service

build-gateway:
	@./scripts/build.sh api-gateway
