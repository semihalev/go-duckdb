// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

import (
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