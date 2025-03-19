package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"time"
)

// ColumnVectorPool implements an efficient pool for column vectors
// This helps minimize allocations during batch query processing
type ColumnVectorPool struct {
	// Type-specific vector pools
	int32Pool   sync.Pool
	int64Pool   sync.Pool
	float64Pool sync.Pool
	boolPool    sync.Pool
	stringPool  sync.Pool
	blobPool    sync.Pool

	// Statistics for monitoring
	gets        uint64
	puts        uint64
	allocations uint64

	// Pool cleanup tracking
	lastCleanup  time.Time
	cleanupMutex sync.Mutex
}

// NewColumnVectorPool creates a new column vector pool
func NewColumnVectorPool() *ColumnVectorPool {
	return &ColumnVectorPool{
		int32Pool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType: C.DUCKDB_TYPE_INTEGER,
					nullMap:    make([]bool, DefaultBatchSize),
					int32Data:  make([]int32, DefaultBatchSize),
					capacity:   DefaultBatchSize,
				}
			},
		},
		int64Pool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType: C.DUCKDB_TYPE_BIGINT,
					nullMap:    make([]bool, DefaultBatchSize),
					int64Data:  make([]int64, DefaultBatchSize),
					capacity:   DefaultBatchSize,
				}
			},
		},
		float64Pool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType:  C.DUCKDB_TYPE_DOUBLE,
					nullMap:     make([]bool, DefaultBatchSize),
					float64Data: make([]float64, DefaultBatchSize),
					capacity:    DefaultBatchSize,
				}
			},
		},
		boolPool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType: C.DUCKDB_TYPE_BOOLEAN,
					nullMap:    make([]bool, DefaultBatchSize),
					boolData:   make([]bool, DefaultBatchSize),
					capacity:   DefaultBatchSize,
				}
			},
		},
		stringPool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType: C.DUCKDB_TYPE_VARCHAR,
					nullMap:    make([]bool, DefaultBatchSize),
					stringData: make([]string, DefaultBatchSize),
					capacity:   DefaultBatchSize,
				}
			},
		},
		blobPool: sync.Pool{
			New: func() interface{} {
				return &ColumnVector{
					columnType: C.DUCKDB_TYPE_BLOB,
					nullMap:    make([]bool, DefaultBatchSize),
					blobData:   make([][]byte, DefaultBatchSize),
					capacity:   DefaultBatchSize,
				}
			},
		},
		lastCleanup: time.Now(),
	}
}

// GetColumnVector gets a column vector for the given type and capacity
func (cp *ColumnVectorPool) GetColumnVector(colType C.duckdb_type, capacity int) *ColumnVector {
	atomic.AddUint64(&cp.gets, 1)

	// Ensure reasonable capacity
	if capacity <= 0 {
		capacity = DefaultBatchSize
	}

	var vector *ColumnVector

	// Get vector from appropriate pool based on type
	switch colType {
	case C.DUCKDB_TYPE_INTEGER:
		vector = cp.int32Pool.Get().(*ColumnVector)

	case C.DUCKDB_TYPE_BIGINT:
		vector = cp.int64Pool.Get().(*ColumnVector)

	case C.DUCKDB_TYPE_DOUBLE:
		vector = cp.float64Pool.Get().(*ColumnVector)

	case C.DUCKDB_TYPE_BOOLEAN:
		vector = cp.boolPool.Get().(*ColumnVector)

	case C.DUCKDB_TYPE_VARCHAR:
		vector = cp.stringPool.Get().(*ColumnVector)

	case C.DUCKDB_TYPE_BLOB:
		vector = cp.blobPool.Get().(*ColumnVector)

	default:
		// For other types, create a new vector
		atomic.AddUint64(&cp.allocations, 1)
		return createColumnVector(colType, capacity)
	}

	// Ensure vector has enough capacity
	if vector.capacity < capacity {
		// Need to resize
		cp.resizeVector(vector, colType, capacity)
		atomic.AddUint64(&cp.allocations, 1)
	}

	// Reset vector state
	cp.resetVector(vector)

	return vector
}

// PutColumnVector returns a column vector to the pool
func (cp *ColumnVectorPool) PutColumnVector(vector *ColumnVector) {
	if vector == nil {
		return
	}

	atomic.AddUint64(&cp.puts, 1)

	// Reset vector to clean state
	cp.resetVector(vector)

	// Return to appropriate pool based on type
	switch vector.columnType {
	case C.DUCKDB_TYPE_INTEGER:
		cp.int32Pool.Put(vector)

	case C.DUCKDB_TYPE_BIGINT:
		cp.int64Pool.Put(vector)

	case C.DUCKDB_TYPE_DOUBLE:
		cp.float64Pool.Put(vector)

	case C.DUCKDB_TYPE_BOOLEAN:
		cp.boolPool.Put(vector)

	case C.DUCKDB_TYPE_VARCHAR:
		cp.stringPool.Put(vector)

	case C.DUCKDB_TYPE_BLOB:
		cp.blobPool.Put(vector)

	default:
		// Other types not pooled
	}

	// Check if cleanup is needed
	cp.maybeCleanup()
}

// resizeVector increases the capacity of a vector
func (cp *ColumnVectorPool) resizeVector(vector *ColumnVector, colType C.duckdb_type, capacity int) {
	// Create new slices with required capacity
	vector.nullMap = make([]bool, capacity)
	vector.capacity = capacity

	// Allocate type-specific data
	switch colType {
	case C.DUCKDB_TYPE_INTEGER:
		vector.int32Data = make([]int32, capacity)

	case C.DUCKDB_TYPE_BIGINT:
		vector.int64Data = make([]int64, capacity)

	case C.DUCKDB_TYPE_DOUBLE:
		vector.float64Data = make([]float64, capacity)

	case C.DUCKDB_TYPE_BOOLEAN:
		vector.boolData = make([]bool, capacity)

	case C.DUCKDB_TYPE_VARCHAR:
		vector.stringData = make([]string, capacity)

	case C.DUCKDB_TYPE_BLOB:
		vector.blobData = make([][]byte, capacity)

	default:
		// For other types, just allocate nullMap
		// Specialized data will be allocated when needed
	}
}

// resetVector resets a vector to clean state for reuse
func (cp *ColumnVectorPool) resetVector(vector *ColumnVector) {
	// Reset length
	vector.length = 0

	// Clear null mask
	for i := range vector.nullMap {
		vector.nullMap[i] = false
	}

	// Clear type-specific data (this helps GC and security)
	switch vector.columnType {
	case C.DUCKDB_TYPE_INTEGER:
		for i := range vector.int32Data {
			vector.int32Data[i] = 0
		}

	case C.DUCKDB_TYPE_BIGINT:
		for i := range vector.int64Data {
			vector.int64Data[i] = 0
		}

	case C.DUCKDB_TYPE_DOUBLE:
		for i := range vector.float64Data {
			vector.float64Data[i] = 0
		}

	case C.DUCKDB_TYPE_BOOLEAN:
		for i := range vector.boolData {
			vector.boolData[i] = false
		}

	case C.DUCKDB_TYPE_VARCHAR:
		for i := range vector.stringData {
			vector.stringData[i] = ""
		}

	case C.DUCKDB_TYPE_BLOB:
		for i := range vector.blobData {
			vector.blobData[i] = nil
		}
	}
}

// maybeCleanup performs periodic maintenance on the pool
func (cp *ColumnVectorPool) maybeCleanup() {
	// Lock for cleanup check
	cp.cleanupMutex.Lock()
	defer cp.cleanupMutex.Unlock()

	// Check if enough time has passed
	now := time.Now()
	if now.Sub(cp.lastCleanup) < 5*time.Minute {
		return // Not time for cleanup yet
	}

	// Mark cleanup time
	cp.lastCleanup = now

	// Pool cleanup happens automatically by Go runtime
	// We just update the timestamp to limit frequency
}

// Stats returns pool statistics
func (cp *ColumnVectorPool) Stats() (gets, puts, allocations uint64) {
	gets = atomic.LoadUint64(&cp.gets)
	puts = atomic.LoadUint64(&cp.puts)
	allocations = atomic.LoadUint64(&cp.allocations)
	return
}

// createColumnVector creates a new column vector for a specific type
// This is a helper function for types not handled by the standard pools
func createColumnVector(colType C.duckdb_type, capacity int) *ColumnVector {
	cv := &ColumnVector{
		columnType: colType,
		nullMap:    make([]bool, capacity),
		capacity:   capacity,
	}

	// Allocate type-specific storage based on column type
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		cv.boolData = make([]bool, capacity)
	case C.DUCKDB_TYPE_TINYINT:
		cv.int8Data = make([]int8, capacity)
	case C.DUCKDB_TYPE_SMALLINT:
		cv.int16Data = make([]int16, capacity)
	case C.DUCKDB_TYPE_INTEGER:
		cv.int32Data = make([]int32, capacity)
	case C.DUCKDB_TYPE_BIGINT:
		cv.int64Data = make([]int64, capacity)
	case C.DUCKDB_TYPE_UTINYINT:
		cv.uint8Data = make([]uint8, capacity)
	case C.DUCKDB_TYPE_USMALLINT:
		cv.uint16Data = make([]uint16, capacity)
	case C.DUCKDB_TYPE_UINTEGER:
		cv.uint32Data = make([]uint32, capacity)
	case C.DUCKDB_TYPE_UBIGINT:
		cv.uint64Data = make([]uint64, capacity)
	case C.DUCKDB_TYPE_FLOAT:
		cv.float32Data = make([]float32, capacity)
	case C.DUCKDB_TYPE_DOUBLE:
		cv.float64Data = make([]float64, capacity)
	case C.DUCKDB_TYPE_VARCHAR:
		cv.stringData = make([]string, capacity)
	case C.DUCKDB_TYPE_BLOB:
		cv.blobData = make([][]byte, capacity)
	default:
		// For complex types (timestamps, etc.), use a generic slice
		cv.timeData = make([]interface{}, capacity)
	}

	return cv
}

// Global pool for column vectors
var globalColumnVectorPool = NewColumnVectorPool()

// GetPooledColumnVector gets a vector from the global pool
func GetPooledColumnVector(colType C.duckdb_type, capacity int) *ColumnVector {
	return globalColumnVectorPool.GetColumnVector(colType, capacity)
}

// PutPooledColumnVector returns a vector to the global pool
func PutPooledColumnVector(vector *ColumnVector) {
	globalColumnVectorPool.PutColumnVector(vector)
}
