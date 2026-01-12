# gRPC API Documentation

## Service Definitions

### ML Service

Service: `ml.MLService`

#### Predict

```protobuf
rpc Predict(PredictRequest) returns (PredictResponse);
```

Request:
```protobuf
message PredictRequest {
  string sensor_id = 1;
  repeated float data = 2;
}
```

Response:
```protobuf
message PredictResponse {
  float prediction = 1;
  float confidence = 2;
}
```

#### BatchPredict

```protobuf
rpc BatchPredict(BatchPredictRequest) returns (BatchPredictResponse);
```

Request:
```protobuf
message BatchPredictRequest {
  repeated PredictRequest requests = 1;
}
```

Response:
```protobuf
message BatchPredictResponse {
  repeated PredictResponse responses = 1;
}
```

## Connection

### Endpoints

- **Dev**: `ml-service.aegisai-dev:9090`
- **Staging**: `ml-service.aegisai-staging:9090`
- **Prod**: `ml-service.aegisai-prod:9090`

### Client Example

```go
conn, err := grpc.Dial("ml-service:9090", grpc.WithInsecure())
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

client := ml.NewMLServiceClient(conn)
resp, err := client.Predict(ctx, &ml.PredictRequest{
    SensorId: "sensor-123",
    Data: []float32{1.0, 2.0, 3.0},
})
```

## Protocol Buffer Generation

Generate Go code from proto files:

```bash
./scripts/generate-proto.sh ml-service
```

## Service Discovery

Services discover each other via Kubernetes DNS:
- Service name: `ml-service`
- Namespace: `aegisai-{env}`
- Full DNS: `ml-service.aegisai-{env}.svc.cluster.local`
