# AegisAI

A production-ready distributed microservices system for processing sensor data with ML inference and alerting capabilities.

## Architecture

AegisAI consists of 5 microservices:

1. **data-generator**: Simulates sensor data and publishes to Kafka
2. **ingestion-service**: Consumes from Kafka, processes data, stores in DynamoDB/S3
3. **ml-service**: Performs ML inference with worker pool, exposes gRPC API
4. **alert-service**: Evaluates thresholds and triggers alerts (can run as Lambda)
5. **api-gateway**: REST and gRPC gateway for frontend and service-to-service communication

## Tech Stack

- **Language**: Go 1.21+
- **Messaging**: Kafka (AWS MSK)
- **Communication**: gRPC, REST
- **Infrastructure**: AWS (EKS, MSK, Lambda, S3, DynamoDB, CloudWatch)
- **IaC**: Terraform
- **Orchestration**: Kubernetes (EKS)
- **CI/CD**: GitHub Actions
- **Monitoring**: Prometheus, Grafana, CloudWatch
- **Frontend**: React

## Quick Start

### Prerequisites

- Go 1.21+
- Docker and Docker Compose
- Terraform >= 1.5.0
- AWS CLI configured
- kubectl (for Kubernetes)

### Local Development

1. **Setup local environment**:
   ```bash
   make setup
   ```

2. **Start services with Docker Compose**:
   ```bash
   make docker-up
   ```

3. **Or run services individually**:
   ```bash
   go run cmd/data-generator/main.go
   go run cmd/ingestion-service/main.go
   # ... etc
   ```

### Building

```bash
# Build all services
make build

# Build specific service
make build-data-generator
```

### Testing

```bash
make test
```

### Deployment

```bash
# Deploy to dev
make deploy ENV=dev

# Deploy to staging
make deploy ENV=staging

# Deploy to prod
make deploy ENV=prod
```

## Project Structure

```
AegisAI/
├── cmd/              # Application entry points
├── services/         # Microservice implementations
├── pkg/              # Shared Go packages
├── frontend/         # React dashboard
├── infrastructure/   # Terraform IaC
├── k8s/              # Kubernetes manifests
├── docker/           # Docker configurations
├── scripts/          # Utility scripts
└── docs/             # Documentation
```

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

## Documentation

- [Architecture](docs/architecture.md)
- [Development Guide](docs/development.md)
- [Deployment Guide](docs/deployment.md)
- [REST API](docs/api/rest.md)
- [gRPC API](docs/api/grpc.md)

## Environments

- **dev**: Development environment
- **staging**: Staging environment
- **prod**: Production environment

Each environment has its own:
- Terraform configuration in `infrastructure/envs/{env}/`
- Kubernetes manifests in `k8s/overlays/{env}/`
- CI/CD workflows in `.github/workflows/`

## Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Run tests and linters
4. Create a pull request

## License

[Add your license here]
