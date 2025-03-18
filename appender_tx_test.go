package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestAppenderWithTransaction(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.FastExec("CREATE TABLE appender_tx_test (id INTEGER, name VARCHAR, age INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test successful transaction
	t.Run("SuccessfulTransaction", func(t *testing.T) {
		// Create appender with transaction
		appender, err := NewAppenderWithTransaction(conn, "", "appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to create appender: %v", err)
		}

		// Append some rows
		for i := 0; i < 10; i++ {
			err = appender.AppendRow(i, "Name "+string(rune(48+i)), 20+i)
			if err != nil {
				t.Fatalf("Failed to append row: %v", err)
			}
		}

		// Commit the transaction
		err = appender.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify rows were inserted
		countResult, err := conn.FastQuery("SELECT COUNT(*) FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer countResult.Close()

		var count int64
		values := make([]driver.Value, 1)
		err = countResult.Next(values)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		count = values[0].(int64)
		if count != 10 {
			t.Errorf("Expected 10 rows, got %d", count)
		}
	})

	// Test rollback
	t.Run("RollbackTransaction", func(t *testing.T) {
		// Clear the table
		_, err = conn.FastExec("DELETE FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to clear table: %v", err)
		}

		// Create appender with transaction
		appender, err := NewAppenderWithTransaction(conn, "", "appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to create appender: %v", err)
		}

		// Append some rows
		for i := 0; i < 5; i++ {
			err = appender.AppendRow(i, "Name "+string(rune(48+i)), 20+i)
			if err != nil {
				t.Fatalf("Failed to append row: %v", err)
			}
		}

		// Rollback the transaction
		err = appender.Rollback()
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		// Verify no rows were inserted
		countResult, err := conn.FastQuery("SELECT COUNT(*) FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer countResult.Close()

		var count int64
		values := make([]driver.Value, 1)
		err = countResult.Next(values)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		count = values[0].(int64)
		if count != 0 {
			t.Errorf("Expected 0 rows after rollback, got %d", count)
		}
	})

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		// Clear the table
		_, err = conn.FastExec("DELETE FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to clear table: %v", err)
		}

		// Create context with cancel
		ctx, cancel := context.WithCancel(context.Background())

		// Create appender with transaction and context
		appender, err := NewAppenderWithTransactionContext(ctx, conn, "", "appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to create appender: %v", err)
		}

		// Append some rows
		for i := 0; i < 5; i++ {
			err = appender.AppendRow(i, "Name "+string(rune(48+i)), 20+i)
			if err != nil {
				t.Fatalf("Failed to append row: %v", err)
			}
		}

		// Cancel context before commit
		cancel()

		// Try to commit with canceled context
		err = appender.CommitContext(ctx)
		if err == nil {
			t.Fatalf("Expected error on commit with canceled context, got nil")
		}

		// Verify no rows were inserted due to context cancellation
		countResult, err := conn.FastQuery("SELECT COUNT(*) FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer countResult.Close()

		var count int64
		values := make([]driver.Value, 1)
		err = countResult.Next(values)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		count = values[0].(int64)
		if count != 0 {
			t.Errorf("Expected 0 rows after context cancellation, got %d", count)
		}
	})

	// Test batch append
	t.Run("BatchAppend", func(t *testing.T) {
		// Clear the table
		_, err = conn.FastExec("DELETE FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to clear table: %v", err)
		}

		// Create appender with transaction
		appender, err := NewAppenderWithTransaction(conn, "", "appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to create appender: %v", err)
		}

		// Create batch data
		ids := make([]int32, 10)
		names := make([]string, 10)
		ages := make([]int32, 10)

		for i := 0; i < 10; i++ {
			ids[i] = int32(i)
			names[i] = "Batch " + string(rune(48+i))
			ages[i] = int32(30 + i)
		}

		// Append batch
		err = appender.AppendBatch([]interface{}{ids, names, ages})
		if err != nil {
			t.Fatalf("Failed to append batch: %v", err)
		}

		// Commit the transaction
		err = appender.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Verify rows were inserted
		countResult, err := conn.FastQuery("SELECT COUNT(*) FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer countResult.Close()

		var count int64
		values := make([]driver.Value, 1)
		err = countResult.Next(values)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		count = values[0].(int64)
		if count != 10 {
			t.Errorf("Expected 10 rows from batch append, got %d", count)
		}
	})

	// Test auto-rollback on Close without Commit
	t.Run("AutoRollbackOnClose", func(t *testing.T) {
		// Clear the table
		_, err = conn.FastExec("DELETE FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to clear table: %v", err)
		}

		// Create appender with transaction
		appender, err := NewAppenderWithTransaction(conn, "", "appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to create appender: %v", err)
		}

		// Append some rows
		for i := 0; i < 5; i++ {
			err = appender.AppendRow(i, "Name "+string(rune(48+i)), 20+i)
			if err != nil {
				t.Fatalf("Failed to append row: %v", err)
			}
		}

		// Close without committing, should rollback
		err = appender.Close()
		if err != nil {
			t.Fatalf("Failed to close appender: %v", err)
		}

		// Verify no rows were inserted
		countResult, err := conn.FastQuery("SELECT COUNT(*) FROM appender_tx_test")
		if err != nil {
			t.Fatalf("Failed to query count: %v", err)
		}
		defer countResult.Close()

		var count int64
		values := make([]driver.Value, 1)
		err = countResult.Next(values)
		if err != nil {
			t.Fatalf("Failed to get count: %v", err)
		}

		count = values[0].(int64)
		if count != 0 {
			t.Errorf("Expected 0 rows after auto-rollback on close, got %d", count)
		}
	})
}
