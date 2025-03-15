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
	"database/sql"
	"database/sql/driver"
	"unsafe"
)

func init() {
	sql.Register("duckdb", &Driver{})
}

// Utility functions for string conversions
func cString(s string) *C.char {
	return C.CString(s)
}

func freeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func goString(s *C.char) string {
	return C.GoString(s)
}

// Driver implements the database/sql/driver.Driver interface.
type Driver struct{}

// Open opens a new connection to the database using memory database as default.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if name == "" {
		name = ":memory:"
	}

	return NewConnection(name)
}