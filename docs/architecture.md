# AegisAI Architecture

## Overview

AegisAI is a distributed microservices system for processing sensor data with ML inference and alerting capabilities.

## System Architecture

### Services

1. **data-generator**: Simulates sensor data and publishes to Kafka
2. **ingestion-service**: Consumes from Kafka, processes data, stores in DynamoDB/S3
3. **ml-service**: Performs ML inference with worker pool, exposes gRPC API
4. **alert-service**: Evaluates thresholds and triggers alerts (can run as Lambda)
5. **api-gateway**: REST and gRPC gateway for frontend and service-to-service communication

### Communication Patterns

- **Async Messaging**: Kafka (AWS MSK) for event-driven communication
- **Synchronous**: gRPC for service-to-service calls
- **Frontend**: REST API via API Gateway

### Infrastructure

- **Compute**: EKS (Kubernetes) for core services, Lambda for lightweight functions
- **Messaging**: AWS MSK (Kafka)
- **Storage**: S3 for artifacts/UI, DynamoDB for structured data
- **Networking**: VPC with public/private subnets
- **Observability**: Prometheus, Grafana, CloudWatch

## Data Flow

1. Data Generator → Kafka (sensor-data topic)
2. Ingestion Service consumes from Kafka → Processes → Stores in DynamoDB/S3
3. ML Service receives inference requests via gRPC → Returns predictions
4. Alert Service monitors thresholds → Triggers alerts
5. API Gateway provides unified REST/gRPC interface

## Deployment

- **Dev**: Single replica, minimal resources
- **Staging**: 2 replicas, moderate resources
- **Prod**: 3+ replicas, production-grade resources
