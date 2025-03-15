// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin/amd64 -lduckdb -lstdc++
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin/arm64 -lduckdb -lstdc++
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux/amd64 -lduckdb -lstdc++
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux/arm64 -lduckdb -lstdc++
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows/amd64 -lduckdb -lstdc++
#cgo windows,arm64 LDFLAGS: -L${SRCDIR}/lib/windows/arm64 -lduckdb -lstdc++

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