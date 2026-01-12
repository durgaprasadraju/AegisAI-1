#!/bin/bash

# Local development environment setup script
# Usage: ./scripts/setup-local.sh

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Setting up local development environment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Start Kafka using Docker Compose
echo "Starting Kafka..."
cd "$ROOT_DIR/docker"
docker-compose -f docker-compose.kafka.yml up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create Kafka topics (if needed)
# docker exec -it kafka kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Setup Go workspace
echo "Setting up Go workspace..."
cd "$ROOT_DIR"
if [ ! -f "go.work" ]; then
    go work init
    for service in services/*/; do
        if [ -f "$service/go.mod" ]; then
            go work use "$service"
        fi
    done
fi

# Install dependencies
echo "Installing Go dependencies..."
for service in services/*/; do
    if [ -f "$service/go.mod" ]; then
        echo "Installing dependencies for $(basename $service)..."
        cd "$service"
        go mod download
        cd "$ROOT_DIR"
    fi
done

echo "✓ Local development environment setup complete!"
echo ""
echo "To start all services:"
echo "  cd docker && docker-compose up"
echo ""
echo "To start only Kafka:"
echo "  cd docker && docker-compose -f docker-compose.kafka.yml up"
