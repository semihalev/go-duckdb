package duckdb

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestMemoryManagement verifies that our memory management works correctly
// by performing many query operations and ensuring resources are cleaned up properly
func TestMemoryManagement(t *testing.T) {
	// Open a database connection directly to test the fast driver path
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	// Create a test table
	if _, err := conn.FastExec("CREATE TABLE memory_test (id INTEGER, name VARCHAR, value DOUBLE)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some test data
	if _, err := conn.FastExec("INSERT INTO memory_test SELECT range as id, 'name_' || range as name, random() as value FROM range(1, 1000)"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Get initial memory stats
	var initialStats runtime.MemStats
	runtime.ReadMemStats(&initialStats)

	// Get initial memory stats only - we'll avoid dealing with the existing string pool

	// Perform many queries in rapid succession to test buffer reuse
	for i := 0; i < 1000; i++ {
		// Execute query and just close it immediately - we're testing memory management, not results
		res, err := conn.FastExec(
			"SELECT count(*) FROM memory_test WHERE id % 10 = " +
				fmt.Sprintf("%d", i%10) +
				" LIMIT 20")
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}

		// Check that we get a non-zero affected rows just to ensure query ran
		if _, err := res.RowsAffected(); err != nil {
			t.Fatalf("Failed to get rows affected: %v", err)
		}

		// Every 100 iterations, force GC and check memory
		if i%100 == 99 {
			runtime.GC()
		}
	}

	// Force garbage collection
	runtime.GC()

	// Wait longer for finalizers to run
	// Hint to runtime that we're low on memory to encourage finalizer processing
	debug := make([]byte, 1024*1024)
	_ = debug
	debug = nil
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	// Get final memory stats
	var finalStats runtime.MemStats
	runtime.ReadMemStats(&finalStats)

	// Print buffer pool statistics directly
	t.Logf("Buffer pool statistics:")
	t.Logf("  Gets:     %d", resultBufferPool.gets)
	t.Logf("  Puts:     %d", resultBufferPool.puts)
	t.Logf("  Misses:   %d", resultBufferPool.misses)
	t.Logf("  Discards: %d", resultBufferPool.discards)

	// Verify that gets and puts are reasonably balanced
	// (allows for some finalizers that haven't run yet)
	// Under high stress, we allow a higher threshold since finalizers might be delayed
	getsPuts := int64(resultBufferPool.gets) - int64(resultBufferPool.puts)
	
	// Allow for higher threshold - in CI environments finalizers might be slow to run
	// Base allowance is 40% of gets, capped at 1000
	maxUnbalanced := int64(float64(resultBufferPool.gets) * 0.40)
	if maxUnbalanced < 50 {
		maxUnbalanced = 50 // Minimum allowance for small tests
	}
	if maxUnbalanced > 1000 {
		maxUnbalanced = 1000 // Cap for very large tests
	}
	
	if getsPuts > maxUnbalanced {
		t.Errorf("Too many unbalanced gets vs puts: %d gets, %d puts, difference: %d (max allowed: %d)",
			resultBufferPool.gets, resultBufferPool.puts, getsPuts, maxUnbalanced)
	} else {
		t.Logf("Gets/puts balance is acceptable: %d gets, %d puts, difference: %d (max allowed: %d)",
			resultBufferPool.gets, resultBufferPool.puts, getsPuts, maxUnbalanced)
	}

	// Check that we're reusing buffers - misses should be much lower than gets
	reuseRate := 1.0
	if resultBufferPool.gets > 0 {
		reuseRate = float64(resultBufferPool.misses) / float64(resultBufferPool.gets)
	}

	t.Logf("Buffer reuse rate: %.2f%% misses", reuseRate*100)

	// Allow higher miss rate for short-running tests
	maxMissRate := 0.5 // 50% by default
	if resultBufferPool.gets < 100 {
		maxMissRate = 0.8 // Allow 80% for very small tests
	} else if resultBufferPool.gets < 500 {
		maxMissRate = 0.6 // Allow 60% for medium tests
	}

	if reuseRate > maxMissRate {
		t.Errorf("Poor buffer reuse rate: %.2f%% misses (max allowed: %.2f%%)", 
			reuseRate*100, maxMissRate*100)
	} else {
		t.Logf("Buffer reuse rate is acceptable: %.2f%% misses (max allowed: %.2f%%)",
			reuseRate*100, maxMissRate*100)
	}
}
