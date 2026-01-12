// +build ignore

// This file tests that all exports are accessible
package main

import "github.com/aegisai/data-generator/internal/generator"

func test() {
	// Test interface
	var _ generator.SensorGenerator = (*generator.RandomSensorGenerator)(nil)
	var _ generator.Publisher = (*generator.InMemoryPublisher)(nil)
	var _ generator.Storage = (*generator.InMemoryStorage)(nil)

	// Test constructors
	_ = generator.NewRandomSensorGenerator(generator.SensorTypeTemperature, 1)
	_ = generator.NewInMemoryPublisher()
	_ = generator.NewInMemoryStorage()

	// Test functions
	_ = generator.GenerateSensorData(generator.SensorTypeTemperature, 1)
	_ = generator.AddReading([]generator.SensorData{}, generator.SensorData{})
	_ = generator.UpdateLatestReadings(map[string]generator.SensorData{}, generator.SensorData{})
}
