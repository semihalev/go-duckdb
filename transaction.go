// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync/atomic"
)

// Transaction represents a DuckDB transaction.
type Transaction struct {
	conn   *Connection
	closed int32
}

// Commit commits the transaction.
func (tx *Transaction) Commit() error {
	if atomic.LoadInt32(&tx.closed) != 0 {
		return fmt.Errorf("transaction is already closed")
	}

	if atomic.LoadInt32(&tx.conn.closed) != 0 {
		return driver.ErrBadConn
	}

	if !atomic.CompareAndSwapInt32(&tx.closed, 0, 1) {
		return fmt.Errorf("transaction is already closed")
	}

	_, err := tx.conn.ExecContext(context.Background(), "COMMIT", nil)
	return err
}

// Rollback aborts the transaction.
func (tx *Transaction) Rollback() error {
	if atomic.LoadInt32(&tx.closed) != 0 {
		return fmt.Errorf("transaction is already closed")
	}

	if atomic.LoadInt32(&tx.conn.closed) != 0 {
		return driver.ErrBadConn
	}

	if !atomic.CompareAndSwapInt32(&tx.closed, 0, 1) {
		return fmt.Errorf("transaction is already closed")
	}

	_, err := tx.conn.ExecContext(context.Background(), "ROLLBACK", nil)
	return err
}