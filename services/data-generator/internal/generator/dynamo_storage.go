package generator

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ============================================================================
// DYNAMO LOCAL STORAGE - Production-Ready DynamoDB Integration
// ============================================================================
// DynamoLocalStorage implements the Storage interface using AWS SDK v2.
// This implementation works with both LocalStack (local) and real AWS DynamoDB.
//
// Why LocalStack mirrors production behavior:
// 1. Same API: LocalStack implements the same DynamoDB API as AWS
// 2. Same SDK: AWS SDK v2 works identically with LocalStack and real DynamoDB
// 3. Same Operations: PutItem, GetItem, Query, BatchWriteItem all work the same
// 4. Same Data Model: Same table structure, keys, and attributes
//
// Migration to production:
// - Set DYNAMO_ENDPOINT to empty string (or omit it) to use real AWS
// - Configure AWS credentials (IAM role, access keys, etc.)
// - No code changes needed
// - Same code works with real DynamoDB

// DynamoLocalStorage stores sensor data in DynamoDB.
// It implements the Storage interface, allowing it to be used interchangeably
// with other storage implementations (e.g., InMemoryStorage for testing).
//
// Table Schema:
//   - Partition Key: SensorID (String)
//   - Sort Key: Timestamp (Number, Unix epoch seconds)
//   - Attributes: Type (String), Value (Number), Unit (String)
//
// This schema allows:
// - Efficient queries by sensor ID
// - Time-series data storage (sorted by timestamp)
// - Latest reading queries (sort by timestamp descending)
type DynamoLocalStorage struct {
	client *dynamodb.Client
	table  string
	config *DynamoConfig
}

// NewDynamoLocalStorage creates a new DynamoDB storage client with the given configuration.
// It initializes an AWS SDK v2 DynamoDB client that can connect to either:
// - LocalStack (when endpoint is set to http://localhost:4566)
// - Real AWS DynamoDB (when endpoint is empty)
//
// The client is configured with:
// - Custom endpoint resolver for LocalStack (if endpoint is provided)
// - Region from config
// - Default credentials (works with LocalStack's dummy credentials)
//
// Example:
//   config := LoadDynamoConfig()
//   storage, err := NewDynamoLocalStorage(config)
//   if err != nil {
//       log.Fatal(err)
//   }
func NewDynamoLocalStorage(dynamoConfig *DynamoConfig) (*DynamoLocalStorage, error) {
	if err := dynamoConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid dynamo config: %w", err)
	}

	// Load AWS config with custom endpoint resolver for LocalStack
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(dynamoConfig.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create DynamoDB client
	clientOptions := []func(*dynamodb.Options){}

	// If endpoint is provided, use it (for LocalStack)
	// If endpoint is empty, use default AWS endpoint (for production)
	if dynamoConfig.Endpoint != "" {
		clientOptions = append(clientOptions, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(dynamoConfig.Endpoint)
		})
	}

	client := dynamodb.NewFromConfig(cfg, clientOptions...)

	return &DynamoLocalStorage{
		client: client,
		table:  dynamoConfig.Table,
		config: dynamoConfig,
	}, nil
}

// Save stores a single sensor reading in DynamoDB.
// It converts SensorData to DynamoDB item format and uses PutItem.
//
// The item structure:
//   - SensorID: partition key (string)
//   - Timestamp: sort key (number, Unix epoch seconds)
//   - Type: sensor type (string)
//   - Value: sensor value (number)
//   - Unit: measurement unit (string)
//
// Context cancellation is respected - if the context is cancelled,
// the operation will return immediately with an error.
//
// This method implements the Storage interface.
func (s *DynamoLocalStorage) Save(ctx context.Context, data SensorData) error {
	// Validate data before saving
	if !data.IsValid() {
		return fmt.Errorf("invalid sensor data: %v", data)
	}

	// Convert SensorData to DynamoDB item
	item := map[string]types.AttributeValue{
		"SensorID":  &types.AttributeValueMemberS{Value: data.ID},
		"Timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", data.Timestamp.Unix())},
		"Type":      &types.AttributeValueMemberS{Value: data.Type},
		"Value":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%.6f", data.Value)},
		"Unit":      &types.AttributeValueMemberS{Value: data.Unit},
	}

	// Put item into DynamoDB
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to save item to dynamodb: %w", err)
	}

	return nil
}

// SaveBatch stores multiple sensor readings in DynamoDB using BatchWriteItem.
// This is more efficient than individual PutItem operations.
//
// BatchWriteItem can handle up to 25 items per request.
// If more items are provided, they are split into multiple batches.
//
// This method implements the Storage interface.
func (s *DynamoLocalStorage) SaveBatch(ctx context.Context, data []SensorData) error {
	if len(data) == 0 {
		return nil // Nothing to save
	}

	// DynamoDB BatchWriteItem can handle up to 25 items per request
	const maxBatchSize = 25

	// Process in batches
	for i := 0; i < len(data); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		if err := s.saveBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to save batch starting at index %d: %w", i, err)
		}
	}

	return nil
}

// saveBatch saves a single batch of items (up to 25 items).
func (s *DynamoLocalStorage) saveBatch(ctx context.Context, data []SensorData) error {
	// Convert SensorData slice to DynamoDB write requests
	writeRequests := make([]types.WriteRequest, 0, len(data))
	for _, reading := range data {
		// Validate each reading
		if !reading.IsValid() {
			return fmt.Errorf("invalid sensor data in batch: %v", reading)
		}

		// Convert to DynamoDB item
		item := map[string]types.AttributeValue{
			"SensorID":  &types.AttributeValueMemberS{Value: reading.ID},
			"Timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", reading.Timestamp.Unix())},
			"Type":      &types.AttributeValueMemberS{Value: reading.Type},
			"Value":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%.6f", reading.Value)},
			"Unit":      &types.AttributeValueMemberS{Value: reading.Unit},
		}

		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
	}

	// Execute batch write
	_, err := s.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			s.table: writeRequests,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to batch write items: %w", err)
	}

	return nil
}

// GetLatest retrieves the most recent reading for a sensor ID.
// It queries DynamoDB with the sensor ID and sorts by timestamp descending.
//
// Query strategy:
// 1. Query by partition key (SensorID)
// 2. Sort by sort key (Timestamp) descending
// 3. Limit to 1 result (latest reading)
//
// Returns:
//   - SensorData: the latest reading (if found)
//   - bool: true if reading was found, false otherwise
//   - error: any error that occurred during the query
//
// This method implements the Storage interface.
func (s *DynamoLocalStorage) GetLatest(ctx context.Context, sensorID string) (SensorData, bool, error) {
	// Query DynamoDB for the latest reading for this sensor
	// We query by partition key (SensorID) and sort by sort key (Timestamp) descending
	result, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("SensorID = :sensorID"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sensorID": &types.AttributeValueMemberS{Value: sensorID},
		},
		ScanIndexForward: aws.Bool(false), // Sort descending (newest first)
		Limit:             aws.Int32(1),    // Only get the latest one
	})
	if err != nil {
		return SensorData{}, false, fmt.Errorf("failed to query dynamodb: %w", err)
	}

	// Check if any items were returned
	if len(result.Items) == 0 {
		return SensorData{}, false, nil
	}

	// Convert DynamoDB item back to SensorData
	// DynamoDB stores fields with different names, so we manually convert
	item := result.Items[0]

	// Extract SensorID
	sensorIDAttr, ok := item["SensorID"]
	if !ok {
		return SensorData{}, false, fmt.Errorf("SensorID not found in dynamodb item")
	}
	sensorIDVal, ok2 := sensorIDAttr.(*types.AttributeValueMemberS)
	if !ok2 {
		return SensorData{}, false, fmt.Errorf("SensorID is not a string")
	}
	sensorIDFromDB := sensorIDVal.Value

	// Extract Timestamp
	timestampAttr, ok := item["Timestamp"]
	if !ok {
		return SensorData{}, false, fmt.Errorf("Timestamp not found in dynamodb item")
	}
	timestampN, ok := timestampAttr.(*types.AttributeValueMemberN)
	if !ok {
		return SensorData{}, false, fmt.Errorf("Timestamp is not a number")
	}
	var timestampUnix int64
	fmt.Sscanf(timestampN.Value, "%d", &timestampUnix)

	// Extract Type
	typeAttr, ok := item["Type"]
	if !ok {
		return SensorData{}, false, fmt.Errorf("Type not found in dynamodb item")
	}
	typeValAttr, ok := typeAttr.(*types.AttributeValueMemberS)
	if !ok {
		return SensorData{}, false, fmt.Errorf("Type is not a string")
	}
	typeVal := typeValAttr.Value

	// Extract Value
	valueAttr, ok := item["Value"]
	if !ok {
		return SensorData{}, false, fmt.Errorf("Value not found in dynamodb item")
	}
	valueN, ok := valueAttr.(*types.AttributeValueMemberN)
	if !ok {
		return SensorData{}, false, fmt.Errorf("Value is not a number")
	}
	var value float64
	fmt.Sscanf(valueN.Value, "%f", &value)

	// Extract Unit
	unitAttr, ok := item["Unit"]
	if !ok {
		return SensorData{}, false, fmt.Errorf("Unit not found in dynamodb item")
	}
	unitAttrVal, ok := unitAttr.(*types.AttributeValueMemberS)
	if !ok {
		return SensorData{}, false, fmt.Errorf("Unit is not a string")
	}
	unit := unitAttrVal.Value

	// Construct SensorData
	data := SensorData{
		ID:        sensorIDFromDB,
		Type:      typeVal,
		Value:     value,
		Timestamp: time.Unix(timestampUnix, 0),
		Unit:      unit,
	}

	return data, true, nil
}

// ============================================================================
// PRODUCTION MIGRATION NOTES
// ============================================================================
//
// To use this with real AWS DynamoDB:
// 1. Set DYNAMO_ENDPOINT to empty string (or omit it)
//    export DYNAMO_ENDPOINT=
// 2. Configure AWS credentials:
//    - IAM role (for EC2/ECS/Lambda)
//    - Access keys (for local development with real AWS)
//    - AWS credentials file (~/.aws/credentials)
// 3. Ensure the table exists in the target region
// 4. No code changes needed!
//
// Table creation (one-time setup):
//   aws dynamodb create-table \
//     --table-name sensor_readings \
//     --attribute-definitions \
//       AttributeName=SensorID,AttributeType=S \
//       AttributeName=Timestamp,AttributeType=N \
//     --key-schema \
//       AttributeName=SensorID,KeyType=HASH \
//       AttributeName=Timestamp,KeyType=RANGE \
//     --billing-mode PAY_PER_REQUEST
//
// The AWS SDK v2 handles:
// - Connection pooling
// - Automatic retries
// - Request signing
// - Error handling
// - All the complexity of DynamoDB protocol
