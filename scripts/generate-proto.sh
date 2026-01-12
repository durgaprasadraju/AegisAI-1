#!/bin/bash

# Protocol buffer generation script
# Usage: ./scripts/generate-proto.sh [service-name]

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Check if protoc is installed
if ! command -v protoc &> /dev/null; then
    echo "Error: protoc is not installed. Please install Protocol Buffers compiler."
    exit 1
fi

# Check if protoc-gen-go is installed
if ! command -v protoc-gen-go &> /dev/null; then
    echo "Installing protoc-gen-go..."
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

generate_proto() {
    local service=$1
    local proto_dir="$ROOT_DIR/services/$service/api/proto"
    local output_dir="$ROOT_DIR/services/$service/internal/grpc"
    
    if [ ! -d "$proto_dir" ]; then
        echo "No proto files found for $service"
        return
    fi
    
    echo "Generating Go code from proto files for $service..."
    
    mkdir -p "$output_dir"
    
    for proto_file in "$proto_dir"/*.proto; do
        if [ -f "$proto_file" ]; then
            protoc \
                --go_out="$output_dir" \
                --go_opt=paths=source_relative \
                --go-grpc_out="$output_dir" \
                --go-grpc_opt=paths=source_relative \
                --proto_path="$proto_dir" \
                "$proto_file"
            
            echo "✓ Generated code from $(basename $proto_file)"
        fi
    done
}

generate_all() {
    local services=("ml-service" "api-gateway")
    
    for service in "${services[@]}"; do
        generate_proto "$service"
    done
}

if [ $# -eq 0 ]; then
    generate_all
else
    generate_proto "$1"
fi

echo "✓ Protocol buffer generation complete!"
