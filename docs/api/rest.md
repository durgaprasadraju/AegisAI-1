# REST API Documentation

## Base URL

- **Dev**: `http://api-gateway.aegisai-dev.local`
- **Staging**: `https://api-staging.aegisai.com`
- **Prod**: `https://api.aegisai.com`

## Endpoints

### Health Check

```
GET /health
```

Returns service health status.

### Sensor Data

```
GET /api/v1/sensors
```

List all sensors.

```
GET /api/v1/sensors/{id}
```

Get sensor details.

```
GET /api/v1/sensors/{id}/data
```

Get sensor data with optional query parameters:
- `start`: Start timestamp
- `end`: End timestamp
- `limit`: Result limit

### ML Inference

```
POST /api/v1/inference
```

Submit data for ML inference.

Request body:
```json
{
  "sensor_id": "sensor-123",
  "data": [1.0, 2.0, 3.0]
}
```

Response:
```json
{
  "prediction": 0.95,
  "confidence": 0.98
}
```

### Alerts

```
GET /api/v1/alerts
```

List all alerts.

```
GET /api/v1/alerts/{id}
```

Get alert details.

## Authentication

API Gateway uses JWT tokens for authentication. Include token in Authorization header:

```
Authorization: Bearer <token>
```

## Rate Limiting

- Dev: 100 requests/minute
- Staging: 1000 requests/minute
- Prod: 10000 requests/minute
