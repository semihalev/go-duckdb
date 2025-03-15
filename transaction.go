package duckdb

import (
	"context"
	"database/sql/driver"
	"sync"
	"sync/atomic"
)

// tx implements database/sql/driver.Tx interface.
type tx struct {
	conn     *conn
	closed   atomic.Bool
	finished atomic.Bool
	mu       sync.Mutex
}

func (tx *tx) Commit() error {
	if tx.closed.Load() {
		return driver.ErrBadConn
	}

	// Ensure we only finish the transaction once
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if tx.finished.Load() {
		return nil
	}

	tx.finished.Store(true)
	_, err := tx.conn.ExecContext(context.Background(), "COMMIT", nil)
	return err
}

func (tx *tx) Rollback() error {
	if tx.closed.Load() {
		return driver.ErrBadConn
	}

	// Ensure we only finish the transaction once
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if tx.finished.Load() {
		return nil
	}

	tx.finished.Store(true)
	_, err := tx.conn.ExecContext(context.Background(), "ROLLBACK", nil)
	return err
}
