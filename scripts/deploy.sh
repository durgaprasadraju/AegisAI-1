#!/bin/bash

# Deployment script for AegisAI services
# Usage: ./scripts/deploy.sh [environment] [service-name]

set -e

ENVIRONMENT=${1:-dev}
SERVICE=${2:-all}
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

deploy_service() {
    local env=$1
    local service=$2
    
    echo "Deploying $service to $env..."
    
    # Build Docker image
    docker build -t "aegisai/$service:$env" -f "$ROOT_DIR/services/$service/Dockerfile" "$ROOT_DIR"
    
    # Push to registry (if configured)
    # docker push "aegisai/$service:$env"
    
    # Deploy to Kubernetes
    kubectl set image deployment/$service $service="aegisai/$service:$env" -n "aegisai-$env"
    
    echo "✓ Deployed $service to $env"
}

deploy_all() {
    local env=$1
    local services=("data-generator" "ingestion-service" "ml-service" "alert-service" "api-gateway")
    
    for service in "${services[@]}"; do
        deploy_service "$env" "$service"
    done
}

if [ "$SERVICE" == "all" ]; then
    deploy_all "$ENVIRONMENT"
else
    deploy_service "$ENVIRONMENT" "$SERVICE"
fi
