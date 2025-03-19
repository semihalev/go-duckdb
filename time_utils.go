// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"time"
)

// DateFromTime converts a Go time.Time to a DuckDB date.
func DateFromTime(t time.Time) C.duckdb_date {
	// Convert time to UTC
	t = t.UTC()

	// Convert to days since Unix epoch (1970-01-01)
	days := int32(t.Unix() / (60 * 60 * 24))

	return C.duckdb_date{days: C.int32_t(days)}
}

// TimeFromDate converts a DuckDB date to a Go time.Time.
func TimeFromDate(date C.duckdb_date) time.Time {
	// Convert days to seconds
	seconds := int64(date.days) * 24 * 60 * 60

	// Create time from Unix timestamp
	return time.Unix(seconds, 0).UTC()
}

// TimeFromTime converts a Go time.Time to a DuckDB time.
func TimeFromTime(t time.Time) C.duckdb_time {
	// Get time components
	hour, min, sec := t.Clock()

	// Calculate microseconds
	micros := int64(hour)*3600*1000000 + int64(min)*60*1000000 + int64(sec)*1000000 + int64(t.Nanosecond())/1000

	return C.duckdb_time{micros: C.int64_t(micros)}
}

// TimeFromDuckDBTime converts a DuckDB time to a Go time.Time.
func TimeFromDuckDBTime(dbTime C.duckdb_time) time.Time {
	// Calculate hours, minutes, seconds, and microseconds
	micros := int64(dbTime.micros)
	seconds := micros / 1000000
	microsRemaining := micros % 1000000

	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	secs := seconds % 60

	// Create a time.Time with the current date and the specified time components
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), int(hours), int(minutes), int(secs), int(microsRemaining*1000), time.UTC)
}

// TimestampFromTime converts a Go time.Time to a DuckDB timestamp.
func TimestampFromTime(t time.Time) C.duckdb_timestamp {
	// Convert to UTC
	t = t.UTC()

	// Calculate microseconds since Unix epoch
	micros := t.Unix()*1000000 + int64(t.Nanosecond())/1000

	return C.duckdb_timestamp{micros: C.int64_t(micros)}
}

// TimeFromTimestamp converts a DuckDB timestamp to a Go time.Time.
func TimeFromTimestamp(ts C.duckdb_timestamp) time.Time {
	// Calculate seconds and nanoseconds
	micros := int64(ts.micros)
	seconds := micros / 1000000
	nanos := (micros % 1000000) * 1000

	// Create time from Unix timestamp with nanoseconds
	return time.Unix(seconds, nanos).UTC()
}
