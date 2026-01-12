#!/bin/bash

# Build script for all AegisAI services
# Usage: ./scripts/build.sh [service-name]

set -e

SERVICES=("data-generator" "ingestion-service" "ml-service" "alert-service" "api-gateway")
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

build_service() {
    local service=$1
    echo "Building $service..."
    
    cd "$ROOT_DIR/services/$service"
    
    # Build Go binary
    go mod download
    go build -o "$ROOT_DIR/bin/$service" ./cmd/$service
    
    echo "✓ Built $service"
}

build_all() {
    echo "Building all services..."
    mkdir -p "$ROOT_DIR/bin"
    
    for service in "${SERVICES[@]}"; do
        build_service "$service"
    done
    
    echo "✓ All services built successfully"
}

if [ $# -eq 0 ]; then
    build_all
else
    build_service "$1"
fi
