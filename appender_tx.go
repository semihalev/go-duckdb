// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"sync/atomic"
)

// AppenderWithTransaction wraps an Appender with transaction support
// to ensure atomicity across multiple append operations.
type AppenderWithTransaction struct {
	appender *Appender
	tx       *Transaction
	conn     *Connection
	closed   int32
	mu       sync.Mutex
}

// NewAppenderWithTransaction creates a new appender with transaction support.
// This ensures that all append operations are committed atomically.
func NewAppenderWithTransaction(conn *Connection, schema, table string) (*AppenderWithTransaction, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Start a transaction
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	// Create appender within the transaction
	appender, err := NewAppender(conn, schema, table)
	if err != nil {
		// Rollback transaction if appender creation fails
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to create appender: %w", err)
	}

	return &AppenderWithTransaction{
		appender: appender,
		tx:       tx.(*Transaction),
		conn:     conn,
	}, nil
}

// NewAppenderWithTransactionContext creates a new appender with transaction support and context.
// This ensures that all append operations are committed atomically and respects context cancellation.
func NewAppenderWithTransactionContext(ctx context.Context, conn *Connection, schema, table string) (*AppenderWithTransaction, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Check context before proceeding
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Start a transaction with context
	tx, err := conn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	// Create appender within the transaction
	appender, err := NewAppender(conn, schema, table)
	if err != nil {
		// Rollback transaction if appender creation fails
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to create appender: %w", err)
	}

	return &AppenderWithTransaction{
		appender: appender,
		tx:       tx.(*Transaction),
		conn:     conn,
	}, nil
}

// AppendRow appends a row with transaction support.
func (a *AppenderWithTransaction) AppendRow(values ...interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appender.AppendRow(values...)
}

// AppendRows appends multiple rows with transaction support.
func (a *AppenderWithTransaction) AppendRows(rows [][]interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appender.AppendRows(rows)
}

// AppendBatch appends a batch of columns with transaction support.
func (a *AppenderWithTransaction) AppendBatch(columns []interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appender.AppendBatch(columns)
}

// Flush flushes all pending changes to the database, while maintaining the transaction.
func (a *AppenderWithTransaction) Flush() error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.appender.Flush()
}

// Commit flushes all pending changes, commits the transaction, and closes the appender.
func (a *AppenderWithTransaction) Commit() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return fmt.Errorf("appender is already closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Flush the appender first
	if err := a.appender.Flush(); err != nil {
		// Try to rollback if flush fails
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to flush appender: %w", err)
	}

	// Close the appender before committing to ensure all data is written
	if err := a.appender.Close(); err != nil {
		// Try to rollback if close fails
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to close appender: %w", err)
	}

	// Commit the transaction
	if err := a.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CommitContext is like Commit but supports context for cancellation.
func (a *AppenderWithTransaction) CommitContext(ctx context.Context) error {
	// First check context
	select {
	case <-ctx.Done():
		// If context is already done, rollback and return error
		_ = a.Rollback()
		return ctx.Err()
	default:
		// Context still valid, proceed
	}

	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return fmt.Errorf("appender is already closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Check context again after lock acquisition
	select {
	case <-ctx.Done():
		// If context is done after lock, rollback and return error
		_ = a.tx.Rollback()
		return ctx.Err()
	default:
		// Context still valid, proceed
	}

	// Flush the appender first
	if err := a.appender.Flush(); err != nil {
		// Try to rollback if flush fails
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to flush appender: %w", err)
	}

	// Check context again after flush
	select {
	case <-ctx.Done():
		// If context is done after flush, rollback and return error
		_ = a.tx.Rollback()
		return ctx.Err()
	default:
		// Context still valid, proceed
	}

	// Close the appender before committing to ensure all data is written
	if err := a.appender.Close(); err != nil {
		// Try to rollback if close fails
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to close appender: %w", err)
	}

	// Commit the transaction
	if err := a.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Rollback aborts the transaction and discards all changes.
func (a *AppenderWithTransaction) Rollback() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return fmt.Errorf("appender is already closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Close the appender first
	if err := a.appender.Close(); err != nil {
		// Try to rollback anyway
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to close appender: %w", err)
	}

	// Roll back the transaction
	if err := a.tx.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

// Close closes the appender and rolls back the transaction if it wasn't committed.
func (a *AppenderWithTransaction) Close() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return nil // Already closed
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Close appender
	if err := a.appender.Close(); err != nil {
		// Try to rollback anyway
		_ = a.tx.Rollback()
		return fmt.Errorf("failed to close appender: %w", err)
	}

	// Roll back transaction (if not already committed)
	if err := a.tx.Rollback(); err != nil {
		// Ignore "transaction already closed" errors
		if err.Error() != "transaction is already closed" {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
	}

	return nil
}
