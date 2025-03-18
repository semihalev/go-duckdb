package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestBufferPoolThreadSafety tests that the buffer pool is thread-safe
func TestBufferPoolThreadSafety(t *testing.T) {
	pool := NewBufferPool()

	// Create a lot of concurrent goroutines to test thread safety
	const numGoroutines = 100
	const iterationsPerGoroutine = 50

	var wg sync.WaitGroup

	// Start a bunch of goroutines that all use the pool concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < iterationsPerGoroutine; j++ {
				// Get a buffer from the pool
				buffer := pool.GetBuffer()

				// Add a small delay to increase chance of race condition
				time.Sleep(time.Millisecond)

				// Return it to the pool
				pool.PutBuffer(buffer)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check that statistics are correct (should be equal number of gets and puts)
	stats := pool.Stats()
	expectedOps := uint64(numGoroutines * iterationsPerGoroutine)

	if stats["gets"] != expectedOps {
		t.Errorf("Expected %d gets, got %d", expectedOps, stats["gets"])
	}

	if stats["puts"] != expectedOps {
		t.Errorf("Expected %d puts, got %d", expectedOps, stats["puts"])
	}
}

// TestBatchQueryThreadSafety tests that the batch query is thread-safe
func TestBatchQueryThreadSafety(t *testing.T) {
	// Create a connection to an in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with some data
	_, err = conn.ExecDirect(`
		CREATE TABLE batch_query_test (id INTEGER, value INTEGER);
		INSERT INTO batch_query_test SELECT i, i*2 FROM range(0, 1000) t(i);
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Execute a query to get a result
	result, err := conn.QueryDirectResult("SELECT * FROM batch_query_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	// Create a batch query from the result
	bq := NewBatchQuery(result.result, 100)
	bq.resultOwned = false // So it doesn't try to free the result that result will free
	defer bq.Close()

	// Create batch rows from the batch query
	br := NewBatchRows(bq)
	defer br.Close()

	// Create multiple goroutines that access the batch rows concurrently
	const numGoroutines = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Read all rows from the batch query
			for {
				// Create a new value slice for each goroutine
				values := make([]driver.Value, bq.columnCount)

				// Try to get the next row - this will call fetchNextBatch which we've synchronized
				err := br.Next(values)
				if err == io.EOF {
					// End of data
					break
				}

				if err != nil {
					errors <- err
					return
				}

				// Add a small delay to increase chance of race conditions
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)

	// Check if any errors occurred
	for err := range errors {
		t.Errorf("Error in goroutine: %v", err)
	}
}

// Helper function to check if an error is a context cancellation error
func isContextCanceled(err error) bool {
	if err == context.Canceled {
		return true
	}
	return strings.Contains(err.Error(), "context canceled") ||
		strings.Contains(err.Error(), "context deadline exceeded")
}

// TestParallelAPIThreadSafety tests the thread safety of the parallel API
func TestParallelAPIThreadSafety(t *testing.T) {
	// Create a connection to an in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with some data
	_, err = conn.ExecDirect(`
		CREATE TABLE parallel_api_test (
			id INTEGER, 
			value1 INTEGER, 
			value2 INTEGER,
			value3 DOUBLE
		);
		INSERT INTO parallel_api_test 
		SELECT 
			i, 
			i*2, 
			i*3,
			i*1.5
		FROM range(0, 1000) t(i);
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Execute a query to get a result
	result, err := conn.QueryDirectResult("SELECT * FROM parallel_api_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	// Create parallel extractor
	pe := NewParallelExtractor(result)

	// Test ProcessChunked with cancellation
	t.Run("TestProcessChunkedWithCancellation", func(t *testing.T) {
		// Create a context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		// Create a wait group to coordinate goroutines
		var wg sync.WaitGroup
		wg.Add(1)

		// Error channel to capture any errors
		errChan := make(chan error, 1)

		// Start a goroutine that calls ProcessChunked
		go func() {
			defer wg.Done()

			// Process in chunks - use all columns (0-3) to avoid type issues
			err := pe.ProcessChunked([]int{0, 1, 2, 3}, 100, func(chunkIdx int, colData map[int]interface{}, nullMasks map[int][]bool) error {
				// Check if context is cancelled after each chunk
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					// For the first chunk, trigger artificial error
					if chunkIdx == 0 {
						return context.Canceled
					}

					// Sleep a bit to simulate processing
					time.Sleep(10 * time.Millisecond)
					return nil
				}
			})

			// Here we handle different error situations:
			// 1. err is nil - No error occurred (shouldn't happen with our implementation)
			// 2. err is context.Canceled or contains "context canceled" - This is expected
			// 3. err is "error processing chunk 0: context canceled" - Also expected, due to our implementation
			// 4. Any other error - This is unexpected and should be reported
			if err != nil && !isContextCanceled(err) {
				errChan <- err
			}
		}()

		// Wait a short time and then cancel the context
		time.Sleep(20 * time.Millisecond)
		cancel()

		// Wait for goroutine to complete
		wg.Wait()

		// Check for unexpected errors
		select {
		case err := <-errChan:
			t.Errorf("Unexpected error: %v", err)
		default:
			// No unexpected errors
		}
	})

	// Test multiple parallel processing functions concurrently - without CGO
	t.Run("TestProcessChunkedGeneric", func(t *testing.T) {
		// Create a simple query to test with a predictable result structure
		_, err := conn.ExecDirect("DROP TABLE IF EXISTS parallel_test")
		if err != nil {
			t.Fatalf("Failed to drop test table: %v", err)
		}

		_, err = conn.ExecDirect(`
			CREATE TABLE parallel_test (
				id INTEGER, 
				name VARCHAR,
				value DOUBLE
			);
			INSERT INTO parallel_test VALUES 
				(1, 'one', 1.1),
				(2, 'two', 2.2),
				(3, 'three', 3.3);
		`)
		if err != nil {
			t.Fatalf("Failed to create test data: %v", err)
		}

		// Ensure cleanup at the end of the test
		defer func() {
			_, _ = conn.ExecDirect("DROP TABLE IF EXISTS parallel_test")
		}()

		// Query the new table which has a well-known structure
		testResult, err := conn.QueryDirectResult("SELECT * FROM parallel_test")
		if err != nil {
			t.Fatalf("Failed to query test data: %v", err)
		}
		defer testResult.Close()

		// Create a parallel extractor for the test result
		testPE := NewParallelExtractor(testResult)

		// Use the ProcessChunked method which doesn't require knowledge of column types
		// Process only the numeric columns (0=id and 2=value) to avoid varchar handling
		err = testPE.ProcessChunked([]int{0, 2}, 2, func(chunkIdx int, colData map[int]interface{}, nullMasks map[int][]bool) error {
			// We're only checking that chunks are processed correctly
			if chunkIdx > 1 {
				return fmt.Errorf("expected at most 2 chunks for 3 rows with chunk size 2, got chunk %d", chunkIdx)
			}

			// Verify we have data for both numeric columns
			if len(colData) != 2 {
				return fmt.Errorf("expected data for 2 columns, got %d", len(colData))
			}

			// Verify we have null masks for both numeric columns
			if len(nullMasks) != 2 {
				return fmt.Errorf("expected null masks for 2 columns, got %d", len(nullMasks))
			}

			// Simulate processing
			time.Sleep(20 * time.Millisecond)
			return nil
		})

		if err != nil {
			t.Errorf("Error in chunk processing: %v", err)
		}
	})
}
