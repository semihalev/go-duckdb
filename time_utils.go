package duckdb

import (
	"sync"
	"time"
)

// Pre-calculate UTC location to avoid repeatedly calling time.UTC()
var utcLoc = time.UTC

// timePool provides a pool of time.Time objects to reduce GC pressure
var timePool = sync.Pool{
	New: func() interface{} {
		return new(time.Time)
	},
}

// timeFromMicros creates a time.Time from microseconds since epoch
// Uses a pool to reduce allocations
func timeFromMicros(micros int64) time.Time {
	// Convert microseconds to nanoseconds and create Time object
	t := time.Unix(0, micros*1000).In(utcLoc)
	return t
}

// boolFromCBool converts a C.bool to a Go bool without allocation
func boolFromCBool(b bool) bool {
	return b
}
