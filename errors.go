// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

import (
	"errors"
	"fmt"
)

// ErrorType represents different types of DuckDB errors.
type ErrorType int

const (
	// ErrGeneric is a generic error.
	ErrGeneric ErrorType = iota
	// ErrConnection is a connection error.
	ErrConnection
	// ErrPrepare is a statement preparation error.
	ErrPrepare
	// ErrExec is a statement execution error.
	ErrExec
	// ErrQuery is a query error.
	ErrQuery
	// ErrType is a type conversion error.
	ErrType
	// ErrBind is a parameter binding error.
	ErrBind
	// ErrAppender is an appender error.
	ErrAppender
	// ErrTransaction is a transaction error.
	ErrTransaction
	// ErrResult is a result set error.
	ErrResult
	// ErrNative is a native optimization error.
	ErrNative
)

// Error is a DuckDB-specific error type.
type Error struct {
	Type    ErrorType
	Message string
	Code    int
}

// Error returns the error message.
func (e *Error) Error() string {
	return fmt.Sprintf("duckdb: %s", e.Message)
}

// NewError creates a new Error.
func NewError(typ ErrorType, message string) *Error {
	return &Error{
		Type:    typ,
		Message: message,
	}
}

// IsError checks if an error is of a specific type.
func IsError(err error, typ ErrorType) bool {
	duckErr, ok := err.(*Error)
	if !ok {
		return false
	}
	return duckErr.Type == typ
}

// Standard errors for the driver API
var (
	// ErrCommitNotSupported is returned when Commit is called during a transaction
	// that was rollback only
	ErrCommitNotSupported = errors.New("commit is not supported - transaction was rollback only")

	// ErrConnectionClosed is returned when a query/statement/transaction is performed on a closed connection
	ErrConnectionClosed = errors.New("connection is closed")

	// ErrStatementClosed is returned when a statement is used after it's closed
	ErrStatementClosed = errors.New("statement is closed")

	// ErrInvalidIsolationLevel is returned when an unsupported isolation level is specified
	ErrInvalidIsolationLevel = errors.New("invalid isolation level")

	// Errors for the optimized native result API

	// ErrResultClosed is returned when attempting to use a closed result
	ErrResultClosed = errors.New("result is closed")

	// ErrInvalidColumnIndex is returned when an invalid column index is specified
	ErrInvalidColumnIndex = errors.New("invalid column index")

	// ErrIncompatibleType is returned when a column's type doesn't match the requested type
	ErrIncompatibleType = errors.New("incompatible column type")

	// ErrNativeLibraryNotFound is returned when the native optimization library can't be loaded
	ErrNativeLibraryNotFound = errors.New("native optimization library not found")

	// ErrNotSupported is returned when functionality is not supported yet
	ErrNotSupported = errors.New("operation not supported")
)
