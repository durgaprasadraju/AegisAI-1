# Development Guide

## Local Development Setup

### 1. Prerequisites

- Go 1.21+
- Docker and Docker Compose
- Protocol Buffers compiler (protoc)
- Make (optional)

### 2. Initial Setup

Run the setup script:

```bash
./scripts/setup-local.sh
```

This will:
- Start local Kafka
- Set up Go workspace
- Install dependencies

### 3. Start Services Locally

#### Option 1: Docker Compose (All Services)

```bash
cd docker
docker-compose up
```

#### Option 2: Run Services Individually

```bash
# Start Kafka only
cd docker
docker-compose -f docker-compose.kafka.yml up -d

# Run services individually
go run cmd/data-generator/main.go
go run cmd/ingestion-service/main.go
go run cmd/ml-service/main.go
go run cmd/alert-service/main.go
go run cmd/api-gateway/main.go
```

### 4. Build Services

```bash
# Build all services
./scripts/build.sh

# Build specific service
./scripts/build.sh data-generator
```

### 5. Generate Protocol Buffers

```bash
# Generate for all services
./scripts/generate-proto.sh

# Generate for specific service
./scripts/generate-proto.sh ml-service
```

## Project Structure

- `cmd/`: Application entry points
- `services/`: Microservice implementations
- `pkg/`: Shared Go packages
- `frontend/`: React dashboard
- `infrastructure/`: Terraform IaC
- `k8s/`: Kubernetes manifests
- `docker/`: Docker configurations
- `scripts/`: Utility scripts

## Development Workflow

1. Create feature branch from `develop`
2. Make changes
3. Run tests: `go test ./...`
4. Build: `./scripts/build.sh`
5. Test locally with Docker Compose
6. Create pull request

## Testing

```bash
# Run all tests
go test ./...

# Run tests for specific service
cd services/data-generator
go test ./...

# Run tests with coverage
go test -cover ./...
```

## Code Style

- Follow Go standard formatting: `go fmt ./...`
- Use `golangci-lint` for linting
- Follow Go best practices and conventions

## Environment Variables

Services use environment variables for configuration:

- `ENVIRONMENT`: dev/staging/prod
- `KAFKA_BROKERS`: Kafka broker addresses
- `AWS_REGION`: AWS region
- Service-specific variables (see each service's config)
