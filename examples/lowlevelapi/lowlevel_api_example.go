// Example demonstrating Go-DuckDB with the low-level API
//
// This example shows how to:
// - Connect to DuckDB using the low-level API
// - Create tables and insert data
// - Execute direct queries
// - Use batch parameter binding
// - Create and use appenders
// - Extract data using column-wise extraction
// - Handle NULL values
// - Process data in parallel
// - Implement a simple analytics pipeline
//
// Run with:
//   go run lowlevel_api_example.go

package main

import (
	"database/sql/driver"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Create a new database connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Basic setup
	fmt.Println("=== Creating schema and sample data ===")
	if err := setupDatabase(conn); err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}

	// Direct query example
	fmt.Println("\n=== Direct query example ===")
	if err := directQueryExample(conn); err != nil {
		log.Fatalf("Failed to run direct query: %v", err)
	}

	// Batch parameter binding example
	fmt.Println("\n=== Batch parameter binding example ===")
	if err := batchParameterExample(conn); err != nil {
		log.Fatalf("Failed to run batch parameter example: %v", err)
	}

	// Appender API example
	fmt.Println("\n=== Appender API example ===")
	if err := appenderExample(conn); err != nil {
		log.Fatalf("Failed to run appender example: %v", err)
	}

	// Column extraction example
	fmt.Println("\n=== Column extraction example ===")
	if err := columnExtractionExample(conn); err != nil {
		log.Fatalf("Failed to run column extraction example: %v", err)
	}

	// NULL handling example
	fmt.Println("\n=== NULL handling example ===")
	if err := nullHandlingExample(conn); err != nil {
		log.Fatalf("Failed to run NULL handling example: %v", err)
	}

	// Parallel processing example
	fmt.Println("\n=== Parallel processing example ===")
	if err := parallelProcessingExample(conn); err != nil {
		log.Fatalf("Failed to run parallel processing example: %v", err)
	}

	// Analytics pipeline example
	fmt.Println("\n=== Analytics pipeline example ===")
	if err := analyticsPipelineExample(conn); err != nil {
		log.Fatalf("Failed to run analytics pipeline example: %v", err)
	}

	fmt.Println("\nAll examples completed successfully!")
}

func setupDatabase(conn *duckdb.Connection) error {
	// Create tables
	createTableQueries := []string{
		// Sensor data table for time-series data
		`CREATE TABLE sensors (
			sensor_id INTEGER,
			timestamp TIMESTAMP,
			temperature DOUBLE,
			humidity DOUBLE,
			pressure DOUBLE,
			battery_level DOUBLE,
			location VARCHAR
		)`,

		// Processed data table for analytics
		`CREATE TABLE processed_data (
			day DATE,
			sensor_id INTEGER,
			avg_temperature DOUBLE,
			min_temperature DOUBLE,
			max_temperature DOUBLE,
			avg_humidity DOUBLE,
			readings INTEGER
		)`,
	}

	for _, query := range createTableQueries {
		_, err := conn.ExecDirect(query)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Insert some initial sensor readings using direct SQL
	// (We'll add more data later using batch and appender methods)
	// Instead of using SQL functions like range, we'll create the sample data with Go code
	// Prepare some sample data
	sampleParams := [][]interface{}{}

	for sensorID := 1; sensorID <= 5; sensorID++ {
		// Determine the location based on sensor ID
		var location string
		switch sensorID % 3 {
		case 0:
			location = "Building A"
		case 1:
			location = "Building B"
		default:
			location = "Building C"
		}

		// Generate 24 hours of data
		for hour := 0; hour < 24; hour++ {
			timestamp := time.Date(2023, 1, 1, hour, 0, 0, 0, time.UTC)
			temperature := 20.0 + rand.Float64()*10.0
			humidity := 50.0 + rand.Float64()*30.0
			pressure := 1000.0 + rand.Float64()*50.0
			batteryLevel := 90.0 + rand.Float64()*10.0

			sampleParams = append(sampleParams, []interface{}{
				sensorID,
				timestamp,
				temperature,
				humidity,
				pressure,
				batteryLevel,
				location,
			})
		}
	}

	// Convert to driver.Value for BatchExec
	driverParams := make([]driver.Value, len(sampleParams))
	for i, params := range sampleParams {
		driverParams[i] = params
	}

	// Insert initial data
	var err error
	_, err = conn.BatchExec(`
		INSERT INTO sensors 
		(sensor_id, timestamp, temperature, humidity, pressure, battery_level, location)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, driverParams)
	if err != nil {
		return fmt.Errorf("failed to insert initial data: %w", err)
	}

	fmt.Println("Created tables and inserted initial sensor readings")
	return nil
}

func directQueryExample(conn *duckdb.Connection) error {
	// Execute a direct query with no parameter binding
	result, err := conn.QueryDirectResult(`
		SELECT 
			sensor_id, 
			location,
			COUNT(*) AS readings,
			AVG(temperature) AS avg_temp,
			MIN(temperature) AS min_temp,
			MAX(temperature) AS max_temp
		FROM 
			sensors
		GROUP BY 
			sensor_id, location
		ORDER BY 
			sensor_id
	`)
	if err != nil {
		return fmt.Errorf("failed to execute direct query: %w", err)
	}
	defer result.Close()

	// Get column count and names for dynamic handling
	columnCount := result.ColumnCount()
	columnNames := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		columnNames[i] = result.ColumnName(i)
	}

	// Now extract each column individually using the appropriate type
	sensorIDs, sensorIDNulls, err := result.ExtractInt32Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract sensor ID column: %w", err)
	}

	locations, locationNulls, err := result.ExtractStringColumn(1)
	if err != nil {
		return fmt.Errorf("failed to extract location column: %w", err)
	}

	readingCounts, readingCountNulls, err := result.ExtractInt64Column(2)
	if err != nil {
		return fmt.Errorf("failed to extract reading count column: %w", err)
	}

	avgTemps, avgTempNulls, err := result.ExtractFloat64Column(3)
	if err != nil {
		return fmt.Errorf("failed to extract avg temp column: %w", err)
	}

	minTemps, minTempNulls, err := result.ExtractFloat64Column(4)
	if err != nil {
		return fmt.Errorf("failed to extract min temp column: %w", err)
	}

	maxTemps, maxTempNulls, err := result.ExtractFloat64Column(5)
	if err != nil {
		return fmt.Errorf("failed to extract max temp column: %w", err)
	}

	// Print results as a table
	fmt.Println("Sensor readings summary:")
	fmt.Printf("%-10s | %-12s | %-10s | %-12s | %-12s | %-12s\n",
		"Sensor ID", "Location", "Readings", "Avg Temp", "Min Temp", "Max Temp")
	fmt.Println("------------------------------------------------------------------------------")

	rowCount := len(sensorIDs)
	for i := 0; i < rowCount; i++ {
		// Check for NULL values (not expected in this query but good practice)
		if sensorIDNulls[i] || locationNulls[i] || readingCountNulls[i] ||
			avgTempNulls[i] || minTempNulls[i] || maxTempNulls[i] {
			continue
		}

		fmt.Printf("%-10d | %-12s | %-10d | %-12.2f | %-12.2f | %-12.2f\n",
			sensorIDs[i], locations[i], readingCounts[i], avgTemps[i], minTemps[i], maxTemps[i])
	}

	return nil
}

func batchParameterExample(conn *duckdb.Connection) error {
	// Demonstrate batch parameter binding for efficient inserts

	// First, create a batch of parameters for sensors
	// We'll insert 3 sensors with 10 readings each
	startTime := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
	batchParams := [][]interface{}{}

	for sensorID := 6; sensorID <= 8; sensorID++ {
		// Determine the location based on sensor ID
		var location string
		switch sensorID % 3 {
		case 0:
			location = "Building A"
		case 1:
			location = "Building B"
		default:
			location = "Building C"
		}

		// Generate 10 readings for each sensor
		for i := 0; i < 10; i++ {
			// Generate some sample data with small random variations
			timestamp := startTime.Add(time.Duration(i) * time.Hour)
			temperature := 20.0 + rand.Float64()*5.0
			humidity := 55.0 + rand.Float64()*10.0
			pressure := 1010.0 + rand.Float64()*10.0
			batteryLevel := 85.0 + rand.Float64()*10.0

			// Add to batch parameters
			batchParams = append(batchParams, []interface{}{
				sensorID,
				timestamp,
				temperature,
				humidity,
				pressure,
				batteryLevel,
				location,
			})
		}
	}

	// Execute the batch insert
	startInsertTime := time.Now()
	// Convert [][]interface{} to []driver.Value by type assertion
	driverBatchParams := make([]driver.Value, len(batchParams))
	for i, params := range batchParams {
		driverBatchParams[i] = params
	}
	result, err := conn.BatchExec(`
		INSERT INTO sensors 
		(sensor_id, timestamp, temperature, humidity, pressure, battery_level, location)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, driverBatchParams)

	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	insertTime := time.Since(startInsertTime)

	fmt.Printf("Inserted %d rows using batch parameter binding in %.2f ms\n",
		rowsAffected, float64(insertTime.Microseconds())/1000.0)

	// Verify the inserted data
	verifyResult, err := conn.QueryDirectResult(`
		SELECT COUNT(*)
		FROM sensors
		WHERE sensor_id BETWEEN 6 AND 8
	`)
	if err != nil {
		return fmt.Errorf("failed to verify batch insert: %w", err)
	}
	defer verifyResult.Close()

	counts, _, err := verifyResult.ExtractInt64Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract count: %w", err)
	}

	fmt.Printf("Verified %d rows in sensors table for sensor IDs 6-8\n", counts[0])
	return nil
}

func appenderExample(conn *duckdb.Connection) error {
	// Demonstrate the high-performance Appender API

	// Create an appender for the sensors table
	appender, err := duckdb.NewAppender(conn, "", "sensors")
	if err != nil {
		return fmt.Errorf("failed to create appender: %w", err)
	}
	defer appender.Close()

	// Generate sample data - 3 sensors with 1000 readings each
	startTime := time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)
	totalRows := 3000
	startInsertTime := time.Now()

	for i := 0; i < totalRows; i++ {
		sensorID := 9 + (i % 3) // Sensor IDs 9, 10, 11

		// Calculate timestamp with 5-minute intervals
		timestamp := startTime.Add(time.Duration(i/3) * 5 * time.Minute)

		// Generate some sample data with small random variations
		temperature := 22.0 + rand.Float64()*8.0
		humidity := 60.0 + rand.Float64()*15.0
		pressure := 1013.0 + rand.Float64()*5.0
		batteryLevel := 75.0 + rand.Float64()*20.0

		// Determine location
		var location string
		switch sensorID % 3 {
		case 0:
			location = "Building A"
		case 1:
			location = "Building B"
		default:
			location = "Building C"
		}

		// Append the row
		err := appender.AppendRow(
			sensorID,
			timestamp,
			temperature,
			humidity,
			pressure,
			batteryLevel,
			location,
		)
		if err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Flush the appender to ensure all data is written
	if err := appender.Flush(); err != nil {
		return fmt.Errorf("failed to flush appender: %w", err)
	}

	insertTime := time.Since(startInsertTime)
	rowsPerSecond := float64(totalRows) / insertTime.Seconds()

	fmt.Printf("Inserted %d rows using the Appender API in %.2f ms (%.0f rows/second)\n",
		totalRows, float64(insertTime.Microseconds())/1000.0, rowsPerSecond)

	// Verify the inserted data
	verifyResult, err := conn.QueryDirectResult(`
		SELECT COUNT(*)
		FROM sensors
		WHERE sensor_id BETWEEN 9 AND 11
	`)
	if err != nil {
		return fmt.Errorf("failed to verify appender insert: %w", err)
	}
	defer verifyResult.Close()

	counts, _, err := verifyResult.ExtractInt64Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract count: %w", err)
	}

	fmt.Printf("Verified %d rows in sensors table for sensor IDs 9-11\n", counts[0])
	return nil
}

func columnExtractionExample(conn *duckdb.Connection) error {
	// Demonstrate efficient column-wise data extraction

	// Create a view with some aggregated data (daily averages)
	_, err := conn.ExecDirect(`
		CREATE VIEW daily_sensor_data AS
		SELECT 
			sensor_id,
			date_trunc('day', timestamp) AS day,
			location,
			AVG(temperature) AS avg_temp,
			MIN(temperature) AS min_temp,
			MAX(temperature) AS max_temp,
			AVG(humidity) AS avg_humidity,
			COUNT(*) AS readings
		FROM 
			sensors
		GROUP BY 
			sensor_id, date_trunc('day', timestamp), location
		ORDER BY 
			day, sensor_id
	`)
	if err != nil {
		return fmt.Errorf("failed to create view: %w", err)
	}

	// Query the view
	startQueryTime := time.Now()

	// First check the column types with a direct query
	typeResult, err := conn.QueryDirectResult(`
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_name = 'daily_sensor_data'
		ORDER BY ordinal_position
	`)
	if err != nil {
		return fmt.Errorf("failed to query column types: %w", err)
	}
	defer typeResult.Close()

	// Print column types for debugging
	colNames, _, err := typeResult.ExtractStringColumn(0)
	if err != nil {
		return fmt.Errorf("failed to extract column names: %w", err)
	}

	colTypes, _, err := typeResult.ExtractStringColumn(1)
	if err != nil {
		return fmt.Errorf("failed to extract column types: %w", err)
	}

	fmt.Println("Column types in daily_sensor_data view:")
	for i := 0; i < len(colNames) && i < len(colTypes); i++ {
		fmt.Printf("  %s: %s\n", colNames[i], colTypes[i])
	}

	// Now query the view data
	result, err := conn.QueryDirectResult(`
		SELECT * FROM daily_sensor_data
		ORDER BY day, sensor_id
	`)
	if err != nil {
		return fmt.Errorf("failed to query daily sensor data: %w", err)
	}
	defer result.Close()

	// Get row count
	rowCount := result.RowCount()

	// Extract columns efficiently
	sensorIDs, sensorIDNulls, err := result.ExtractInt32Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract sensor IDs: %w", err)
	}

	// Let's query directly again with CAST for the date column
	result.Close() // Close the previous result

	result, err = conn.QueryDirectResult(`
		SELECT 
			sensor_id,
			CAST(day AS TIMESTAMP) AS day,
			location,
			avg_temp,
			min_temp,
			max_temp,
			avg_humidity,
			readings
		FROM daily_sensor_data
		ORDER BY day, sensor_id
	`)
	if err != nil {
		return fmt.Errorf("failed to query daily sensor data with cast: %w", err)
	}
	defer result.Close()

	// Extract columns again after the CAST
	sensorIDs, sensorIDNulls, err = result.ExtractInt32Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract sensor IDs (second query): %w", err)
	}

	// Now we can extract the day as a timestamp
	timestamps, timestampNulls, err := result.ExtractTimestampColumn(1)
	if err != nil {
		return fmt.Errorf("failed to extract days: %w", err)
	}

	// Convert timestamps to time.Time objects
	days := make([]time.Time, len(timestamps))
	dayNulls := make([]bool, len(timestampNulls))
	copy(dayNulls, timestampNulls)

	for i, ts := range timestamps {
		if !timestampNulls[i] {
			days[i] = duckdb.ConvertTimestampToTime(ts)
		}
	}

	locations, _, err := result.ExtractStringColumn(2)
	if err != nil {
		return fmt.Errorf("failed to extract locations: %w", err)
	}

	avgTemps, avgTempNulls, err := result.ExtractFloat64Column(3)
	if err != nil {
		return fmt.Errorf("failed to extract avg temperatures: %w", err)
	}

	minTemps, _, err := result.ExtractFloat64Column(4)
	if err != nil {
		return fmt.Errorf("failed to extract min temperatures: %w", err)
	}

	maxTemps, _, err := result.ExtractFloat64Column(5)
	if err != nil {
		return fmt.Errorf("failed to extract max temperatures: %w", err)
	}

	avgHumidities, _, err := result.ExtractFloat64Column(6)
	if err != nil {
		return fmt.Errorf("failed to extract avg humidities: %w", err)
	}

	readingCounts, _, err := result.ExtractInt64Column(7)
	if err != nil {
		return fmt.Errorf("failed to extract reading counts: %w", err)
	}

	extractionTime := time.Since(startQueryTime)

	// Store the summarized data to the processed_data table
	// Use NewAppenderWithTransaction which manages its own internal transaction
	appender, err := duckdb.NewAppenderWithTransaction(conn, "", "processed_data")
	if err != nil {
		return fmt.Errorf("failed to create appender: %w", err)
	}

	// Insert each row of the result into the processed_data table
	rowCountInt := int(rowCount)
	for i := 0; i < rowCountInt; i++ {
		// Skip rows with NULL values in key columns
		if sensorIDNulls[i] || dayNulls[i] || avgTempNulls[i] {
			continue
		}

		err := appender.AppendRow(
			days[i],
			sensorIDs[i],
			avgTemps[i],
			minTemps[i],
			maxTemps[i],
			avgHumidities[i],
			readingCounts[i],
		)
		if err != nil {
			appender.Rollback()
			appender.Close()
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Close the appender which will automatically commit its internal transaction
	appender.Close()

	// Show some statistics about the operation
	fmt.Printf("Extracted and processed %d rows in %.2f ms\n",
		rowCount, float64(extractionTime.Microseconds())/1000.0)
	fmt.Printf("Stored %d daily sensor readings in processed_data table\n", rowCount)

	// Print the first few rows to verify
	fmt.Println("\nSample of daily sensor data:")
	fmt.Println("--------------------------------------------------")
	fmt.Printf("%-10s | %-10s | %-12s | %-10s | %-10s\n",
		"Date", "Sensor ID", "Location", "Avg Temp", "Readings")
	fmt.Println("--------------------------------------------------")

	displayRows := 5
	if rowCountInt < displayRows {
		displayRows = rowCountInt
	}

	for i := 0; i < displayRows; i++ {
		// Format the date for display (extract just the date part)
		dateStr := days[i].Format("2006-01-02")

		fmt.Printf("%-10s | %-10d | %-12s | %-10.2f | %-10d\n",
			dateStr, sensorIDs[i], locations[i], avgTemps[i], readingCounts[i])
	}

	if rowCountInt > displayRows {
		fmt.Printf("... and %d more rows\n", rowCountInt-displayRows)
	}

	return nil
}

func nullHandlingExample(conn *duckdb.Connection) error {
	// Create a table with NULL values
	_, err := conn.ExecDirect(`
		CREATE TABLE sensor_failures (
			failure_id INTEGER,
			sensor_id INTEGER,
			timestamp TIMESTAMP,
			error_code VARCHAR,
			temperature DOUBLE,  -- NULL if temperature sensor failed
			humidity DOUBLE,     -- NULL if humidity sensor failed
			pressure DOUBLE,     -- NULL if pressure sensor failed
			recovery_time TIMESTAMP -- NULL if not yet recovered
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create sensor_failures table: %w", err)
	}

	// Insert sample data with NULL values
	sampleData := `
		INSERT INTO sensor_failures VALUES
		(1, 1, TIMESTAMP '2023-01-15 08:30:00', 'TEMP_SENSOR_ERROR', NULL, 65.2, 1008.3, TIMESTAMP '2023-01-15 10:15:00'),
		(2, 3, TIMESTAMP '2023-01-17 14:20:00', 'HUMIDITY_SENSOR_ERROR', 22.4, NULL, 1012.8, TIMESTAMP '2023-01-17 18:45:00'),
		(3, 5, TIMESTAMP '2023-01-22 23:10:00', 'PRESSURE_SENSOR_ERROR', 18.7, 58.9, NULL, TIMESTAMP '2023-01-23 05:30:00'),
		(4, 2, TIMESTAMP '2023-02-05 11:45:00', 'COMPLETE_FAILURE', NULL, NULL, NULL, TIMESTAMP '2023-02-05 16:20:00'),
		(5, 7, TIMESTAMP '2023-02-10 09:15:00', 'HUMIDITY_SENSOR_ERROR', 21.3, NULL, 1009.5, NULL)
	`
	_, err = conn.ExecDirect(sampleData)
	if err != nil {
		return fmt.Errorf("failed to insert sample failure data: %w", err)
	}

	// Query the data
	result, err := conn.QueryDirectResult(`
		SELECT 
			failure_id,
			sensor_id,
			timestamp,
			error_code,
			temperature,
			humidity,
			pressure,
			recovery_time,
			CASE WHEN recovery_time IS NULL THEN 'Ongoing' ELSE 'Resolved' END AS status
		FROM 
			sensor_failures
		ORDER BY 
			timestamp
	`)
	if err != nil {
		return fmt.Errorf("failed to query sensor failures: %w", err)
	}
	defer result.Close()

	// Extract columns
	failureIDs, failureIDNulls, err := result.ExtractInt32Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract failure IDs: %w", err)
	}

	sensorIDs, sensorIDNulls, err := result.ExtractInt32Column(1)
	if err != nil {
		return fmt.Errorf("failed to extract sensor IDs: %w", err)
	}

	timestamps, timestampNulls, err := result.ExtractTimeColumn(2)
	if err != nil {
		return fmt.Errorf("failed to extract timestamps: %w", err)
	}

	errorCodes, errorCodeNulls, err := result.ExtractStringColumn(3)
	if err != nil {
		return fmt.Errorf("failed to extract error codes: %w", err)
	}

	temperatures, temperatureNulls, err := result.ExtractFloat64Column(4)
	if err != nil {
		return fmt.Errorf("failed to extract temperatures: %w", err)
	}

	humidities, humidityNulls, err := result.ExtractFloat64Column(5)
	if err != nil {
		return fmt.Errorf("failed to extract humidities: %w", err)
	}

	pressures, pressureNulls, err := result.ExtractFloat64Column(6)
	if err != nil {
		return fmt.Errorf("failed to extract pressures: %w", err)
	}

	_, _, err = result.ExtractTimeColumn(7)
	if err != nil {
		return fmt.Errorf("failed to extract recovery times: %w", err)
	}

	statuses, statusNulls, err := result.ExtractStringColumn(8)
	if err != nil {
		return fmt.Errorf("failed to extract statuses: %w", err)
	}

	// Display results with proper NULL handling
	fmt.Println("Sensor failure report:")
	fmt.Println("--------------------------------------------------------------------------------------")
	fmt.Printf("%-3s | %-3s | %-19s | %-22s | %-5s | %-5s | %-5s | %-8s\n",
		"ID", "SID", "Timestamp", "Error", "Temp", "Hum", "Pres", "Status")
	fmt.Println("--------------------------------------------------------------------------------------")

	rowCount := len(failureIDs)
	for i := 0; i < rowCount; i++ {
		// Check for NULL values in key columns (shouldn't happen in this example)
		if failureIDNulls[i] || sensorIDNulls[i] || timestampNulls[i] || errorCodeNulls[i] || statusNulls[i] {
			continue
		}

		// Format timestamp
		timestampStr := timestamps[i].Format("2006-01-02 15:04:05")

		// Handle NULL values in sensor readings
		tempStr := "NULL"
		if !temperatureNulls[i] {
			tempStr = fmt.Sprintf("%.1f", temperatures[i])
		}

		humStr := "NULL"
		if !humidityNulls[i] {
			humStr = fmt.Sprintf("%.1f", humidities[i])
		}

		presStr := "NULL"
		if !pressureNulls[i] {
			presStr = fmt.Sprintf("%.1f", pressures[i])
		}

		fmt.Printf("%-3d | %-3d | %-19s | %-22s | %-5s | %-5s | %-5s | %-8s\n",
			failureIDs[i], sensorIDs[i], timestampStr, errorCodes[i],
			tempStr, humStr, presStr, statuses[i])
	}

	return nil
}

func parallelProcessingExample(conn *duckdb.Connection) error {
	// This example demonstrates how to process extracted column data in parallel

	// Query for large dataset
	result, err := conn.QueryDirectResult(`
		SELECT 
			sensor_id,
			timestamp,
			temperature,
			humidity,
			pressure
		FROM 
			sensors
		WHERE
			timestamp >= TIMESTAMP '2023-03-01'
		ORDER BY 
			sensor_id, timestamp
	`)
	if err != nil {
		return fmt.Errorf("failed to query sensor data: %w", err)
	}
	defer result.Close()

	// Extract columns
	sensorIDs, _, err := result.ExtractInt32Column(0)
	if err != nil {
		return fmt.Errorf("failed to extract sensor IDs: %w", err)
	}

	_, _, err = result.ExtractTimeColumn(1)
	if err != nil {
		return fmt.Errorf("failed to extract timestamps: %w", err)
	}

	temperatures, tempNulls, err := result.ExtractFloat64Column(2)
	if err != nil {
		return fmt.Errorf("failed to extract temperatures: %w", err)
	}

	humidities, humidityNulls, err := result.ExtractFloat64Column(3)
	if err != nil {
		return fmt.Errorf("failed to extract humidities: %w", err)
	}

	pressures, pressureNulls, err := result.ExtractFloat64Column(4)
	if err != nil {
		return fmt.Errorf("failed to extract pressures: %w", err)
	}

	rowCount := len(sensorIDs)
	fmt.Printf("Processing %d sensor readings in parallel\n", rowCount)

	// Define worker count based on number of rows
	workerCount := 4
	if rowCount < 1000 {
		workerCount = 2
	}

	// Split the data for parallel processing
	recordsPerWorker := rowCount / workerCount

	// Process the data in parallel
	startProcessingTime := time.Now()
	var wg sync.WaitGroup

	// Create channels for results
	type SensorSummary struct {
		SensorID               int32
		RecordCount            int
		AverageTemperature     float64
		TemperatureCorrelation float64 // Correlation between temp and humidity
	}
	resultChannel := make(chan SensorSummary, workerCount)

	// Launch workers
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Determine this worker's data range
			startIdx := workerID * recordsPerWorker
			endIdx := startIdx + recordsPerWorker
			if workerID == workerCount-1 {
				endIdx = rowCount // Last worker takes remaining rows
			}

			// Group data by sensor ID within this worker's range
			sensorData := make(map[int32]struct {
				Count        int
				SumTemp      float64
				SumPressure  float64
				TempHumPairs []struct {
					Temp float64
					Hum  float64
				}
			})

			// First pass: gather data by sensor
			for i := startIdx; i < endIdx; i++ {
				// Skip rows with NULL values in key columns
				if tempNulls[i] || humidityNulls[i] || pressureNulls[i] {
					continue
				}

				sensorID := sensorIDs[i]
				data, exists := sensorData[sensorID]
				if !exists {
					data = struct {
						Count        int
						SumTemp      float64
						SumPressure  float64
						TempHumPairs []struct {
							Temp float64
							Hum  float64
						}
					}{
						TempHumPairs: make([]struct {
							Temp float64
							Hum  float64
						}, 0, 100),
					}
				}

				data.Count++
				data.SumTemp += temperatures[i]
				data.SumPressure += pressures[i]
				data.TempHumPairs = append(data.TempHumPairs, struct {
					Temp float64
					Hum  float64
				}{
					Temp: temperatures[i],
					Hum:  humidities[i],
				})

				sensorData[sensorID] = data
			}

			// Second pass: calculate statistics for each sensor
			for sensorID, data := range sensorData {
				if data.Count < 2 {
					continue
				}

				// Calculate average temperature
				avgTemp := data.SumTemp / float64(data.Count)

				// Calculate correlation between temperature and humidity
				// Using Pearson correlation coefficient
				var sumX, sumY, sumXY, sumX2, sumY2 float64
				for _, pair := range data.TempHumPairs {
					sumX += pair.Temp
					sumY += pair.Hum
					sumXY += pair.Temp * pair.Hum
					sumX2 += pair.Temp * pair.Temp
					sumY2 += pair.Hum * pair.Hum
				}

				n := float64(len(data.TempHumPairs))
				correlation := 0.0
				denominator := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))
				if denominator > 0 {
					correlation = (n*sumXY - sumX*sumY) / denominator
				}

				// Send results
				resultChannel <- SensorSummary{
					SensorID:               sensorID,
					RecordCount:            data.Count,
					AverageTemperature:     avgTemp,
					TemperatureCorrelation: correlation,
				}
			}
		}(w)
	}

	// Wait for all processing to complete then close the result channel
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	// Collect and display results
	fmt.Println("\nParallel processing results:")
	fmt.Println("-----------------------------------------------------------")
	fmt.Printf("%-10s | %-12s | %-18s | %-20s\n",
		"Sensor ID", "Record Count", "Avg Temperature", "Temp-Humidity Corr")
	fmt.Println("-----------------------------------------------------------")

	summaries := make([]SensorSummary, 0)
	for summary := range resultChannel {
		summaries = append(summaries, summary)
	}

	// Sort by sensor ID for consistent output
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].SensorID < summaries[j].SensorID
	})

	for _, summary := range summaries {
		fmt.Printf("%-10d | %-12d | %-18.2f | %-20.4f\n",
			summary.SensorID,
			summary.RecordCount,
			summary.AverageTemperature,
			summary.TemperatureCorrelation)
	}

	processingTime := time.Since(startProcessingTime)
	fmt.Printf("\nProcessed %d records across %d sensors in %.2f ms using %d workers\n",
		rowCount, len(summaries), float64(processingTime.Microseconds())/1000.0, workerCount)

	return nil
}

func analyticsPipelineExample(conn *duckdb.Connection) error {
	// This example demonstrates a complete analytics pipeline using DuckDB's features

	// First, create more synthetic data for demonstration
	fmt.Println("Generating and loading additional sensor data...")

	// Create a large batch of synthetic data
	const batchSize = 10000
	startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	batchParams := make([][]interface{}, batchSize)

	for i := 0; i < batchSize; i++ {
		sensorID := 1 + (i % 10) // 10 different sensors

		// Calculate timestamp with varying intervals
		dayOffset := i / 100
		timeOffset := i % 100
		timestamp := startTime.AddDate(0, 0, dayOffset).Add(time.Duration(timeOffset) * 15 * time.Minute)

		// Generate somewhat realistic seasonal temperatures
		baseTemp := 20.0
		seasonalOffset := 10.0 * math.Sin(2*math.Pi*float64(dayOffset)/365.0)
		tempTimeOffset := 5.0 * math.Sin(2*math.Pi*float64(timeOffset)/96.0)
		randomOffset := rand.Float64()*3.0 - 1.5
		temperature := baseTemp + seasonalOffset + tempTimeOffset + randomOffset

		// Humidity is inversely related to temperature with some randomness
		humidity := 80.0 - temperature*1.5 + rand.Float64()*15.0
		humidity = math.Max(20.0, math.Min(95.0, humidity)) // Clamp to realistic range

		// Pressure varies less but has some patterns
		pressureBase := 1013.0
		pressureDaily := 2.0 * math.Sin(2*math.Pi*float64(timeOffset)/96.0)
		pressureRandom := rand.Float64()*4.0 - 2.0
		pressure := pressureBase + pressureDaily + pressureRandom

		// Battery level decreases over time with periodic recharges
		batteryLevel := 100.0 - math.Mod(float64(dayOffset*timeOffset)*0.05, 30.0)

		// Location depends on sensor ID
		var location string
		switch sensorID % 5 {
		case 0:
			location = "Building A"
		case 1:
			location = "Building B"
		case 2:
			location = "Building C"
		case 3:
			location = "Building D"
		case 4:
			location = "Building E"
		}

		batchParams[i] = []interface{}{
			sensorID,
			timestamp,
			temperature,
			humidity,
			pressure,
			batteryLevel,
			location,
		}
	}

	// Load the batch data
	startLoadTime := time.Now()
	// Convert [][]interface{} to []driver.Value by type assertion
	driverBatchParams := make([]driver.Value, len(batchParams))
	for i, params := range batchParams {
		driverBatchParams[i] = params
	}
	_, err := conn.BatchExec(`
		INSERT INTO sensors 
		(sensor_id, timestamp, temperature, humidity, pressure, battery_level, location)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, driverBatchParams)
	if err != nil {
		return fmt.Errorf("failed to load batch data: %w", err)
	}
	loadTime := time.Since(startLoadTime)

	fmt.Printf("Loaded %d records in %.2f ms\n", batchSize, float64(loadTime.Microseconds())/1000.0)

	// Step 1: Execute an analytics pipeline using DuckDB's SQL capabilities
	fmt.Println("\nExecuting analytics pipeline...")
	startPipelineTime := time.Now()

	// Using SQL pipeline for complex analytics
	result, err := conn.QueryDirectResult(`
		WITH sensor_stats AS (
			-- Calculate daily statistics for each sensor and location
			SELECT 
				sensor_id,
				date_trunc('day', timestamp) AS day,
				location,
				COUNT(*) AS reading_count,
				AVG(temperature) AS avg_temp,
				MIN(temperature) AS min_temp,
				MAX(temperature) AS max_temp,
				AVG(humidity) AS avg_humidity,
				AVG(pressure) AS avg_pressure,
				MIN(battery_level) AS min_battery
			FROM 
				sensors
			GROUP BY 
				sensor_id, date_trunc('day', timestamp), location
		),
		location_stats AS (
			-- Aggregate by location
			SELECT 
				location,
				day,
				COUNT(DISTINCT sensor_id) AS sensor_count,
				AVG(avg_temp) AS location_avg_temp,
				MIN(min_temp) AS location_min_temp,
				MAX(max_temp) AS location_max_temp,
				SUM(reading_count) AS total_readings
			FROM 
				sensor_stats
			GROUP BY 
				location, day
		),
		abnormal_readings AS (
			-- Detect days with abnormal temperature variations
			SELECT 
				ss.day,
				ss.location,
				ss.sensor_id,
				ss.avg_temp,
				ls.location_avg_temp,
				ABS(ss.avg_temp - ls.location_avg_temp) AS temp_deviation,
				CASE
					WHEN ABS(ss.avg_temp - ls.location_avg_temp) > 5.0 THEN 'High'
					WHEN ABS(ss.avg_temp - ls.location_avg_temp) > 2.5 THEN 'Medium'
					ELSE 'Low'
				END AS deviation_level
			FROM 
				sensor_stats ss
				JOIN location_stats ls ON ss.location = ls.location AND ss.day = ls.day
			WHERE 
				ls.sensor_count > 1 -- Need multiple sensors for meaningful comparison
		)
		-- Final aggregated output
		SELECT 
			location,
			CAST(DATE_PART('month', day) AS INTEGER) AS month,
			COUNT(DISTINCT day) AS days_with_data,
			ROUND(AVG(location_avg_temp), 1) AS avg_temp,
			CAST(SUM(CASE WHEN deviation_level = 'High' THEN 1 ELSE 0 END) AS BIGINT) AS high_deviations,
			CAST(SUM(CASE WHEN deviation_level = 'Medium' THEN 1 ELSE 0 END) AS BIGINT) AS medium_deviations,
			COUNT(*) AS total_readings
		FROM 
			abnormal_readings
		GROUP BY 
			location, CAST(DATE_PART('month', day) AS INTEGER)
		ORDER BY 
			location, month
	`)
	if err != nil {
		return fmt.Errorf("failed to execute analytics pipeline: %w", err)
	}
	defer result.Close()

	pipelineTime := time.Since(startPipelineTime)

	// We'll keep this simple and use the following data types:
	// 0: location - VARCHAR
	// 1: month - INTEGER
	// 2: days_with_data - BIGINT
	// 3: avg_temp - DOUBLE
	// 4: high_deviations - BIGINT
	// 5: medium_deviations - BIGINT
	// 6: total_readings - BIGINT

	// Extract the results
	locations, _, err := result.ExtractStringColumn(0)
	if err != nil {
		return fmt.Errorf("failed to extract locations: %w", err)
	}

	// Now extract using the correct types
	months, _, err := result.ExtractInt32Column(1)
	if err != nil {
		return fmt.Errorf("failed to extract months: %w", err)
	}

	daysWithData, _, err := result.ExtractInt64Column(2)
	if err != nil {
		return fmt.Errorf("failed to extract days with data: %w", err)
	}

	avgTemps, _, err := result.ExtractFloat64Column(3)
	if err != nil {
		return fmt.Errorf("failed to extract average temperatures: %w", err)
	}

	highDeviations, _, err := result.ExtractInt64Column(4)
	if err != nil {
		return fmt.Errorf("failed to extract high deviations: %w", err)
	}

	mediumDeviations, _, err := result.ExtractInt64Column(5)
	if err != nil {
		return fmt.Errorf("failed to extract medium deviations: %w", err)
	}

	totalReadings, _, err := result.ExtractInt64Column(6)
	if err != nil {
		return fmt.Errorf("failed to extract total readings: %w", err)
	}

	// Print the results
	fmt.Printf("Analytics pipeline completed in %.2f ms\n", float64(pipelineTime.Microseconds())/1000.0)
	fmt.Println("\nMonthly Temperature Analysis by Location:")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("%-12s | %-5s | %-8s | %-10s | %-14s | %-15s | %-8s\n",
		"Location", "Month", "Days", "Avg Temp", "High Devs", "Medium Devs", "Readings")
	fmt.Println("--------------------------------------------------------------------------------")

	rowCount := len(locations)
	for i := 0; i < rowCount; i++ {
		// Convert month number to name
		monthName := time.Month(months[i]).String()[:3]

		fmt.Printf("%-12s | %-5s | %-8d | %-10.1f | %-14d | %-15d | %-8d\n",
			locations[i], monthName, daysWithData[i], avgTemps[i],
			highDeviations[i], mediumDeviations[i], totalReadings[i])
	}

	// Final summary
	fmt.Println("\nAnalytics Pipeline Summary:")
	fmt.Printf("- Processed data for %d location-month combinations\n", rowCount)
	fmt.Printf("- Pipeline execution time: %.2f ms\n", float64(pipelineTime.Microseconds())/1000.0)
	fmt.Printf("- Total data load time: %.2f ms\n", float64(loadTime.Microseconds())/1000.0)
	fmt.Printf("- Total sensor readings processed: %d\n", batchSize)

	return nil
}
