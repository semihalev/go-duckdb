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
	"strings"
)

// Version information
const (
	// DriverVersion is the version of this driver
	DriverVersion = "0.1.0"
)

// DriverName is the name of this driver
const DriverName = "go-duckdb"

// GetDuckDBVersion returns the version string of the DuckDB library.
func GetDuckDBVersion() string {
	return goString(C.duckdb_library_version())
}

// CheckDuckDBVersion checks if the current DuckDB version is compatible with the required version.
func CheckDuckDBVersion(required string) bool {
	current := GetDuckDBVersion()
	
	// Simple version check - we just look at the prefix
	return strings.HasPrefix(current, required)
}

// GetDuckDBFeatureFlags returns a map of feature flags for the current DuckDB version.
// This is useful for runtime feature detection.
func GetDuckDBFeatureFlags() map[string]bool {
	version := GetDuckDBVersion()
	
	flags := map[string]bool{
		"AppenderSupport":    true,                     // All versions support appenders
		"PreparedStatements": !strings.HasPrefix(version, "1.2"), // Prepared statements have issues in 1.2.x
		"NamedParameters":    !strings.HasPrefix(version, "1.2"), // Named parameters have issues in 1.2.x
		"BlobSupport":        true,                     // All versions support BLOBs
		"TimeZoneSupport":    strings.HasPrefix(version, "1.2"),  // Time zone types added in 1.2.x
	}
	
	return flags
}