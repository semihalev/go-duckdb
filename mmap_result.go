package duckdb

/*
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include "mmap/mmap_adapter.h"

// Include C implementation directly
#include "mmap/mmap_adapter.c"
*/
import "C"
import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// DataType corresponds to C enum mmap_data_type_t
type DataType int

// Data types from mmap_adapter.h
const (
	TypeNull DataType = iota
	TypeBoolean
	TypeInt8
	TypeInt16
	TypeInt32
	TypeInt64
	TypeUint8
	TypeUint16
	TypeUint32
	TypeUint64
	TypeFloat
	TypeDouble
	TypeVarchar
	TypeBlob
	TypeDate
	TypeTime
	TypeTimestamp
	TypeInterval
	TypeHugeInt
	TypeDecimal
	TypeUUID
	TypeStruct
	TypeList
	TypeMap
	TypeEnum
	TypeBit
)

// MMapResult represents a memory-mapped result from DuckDB
// This provides a direct view into the memory-mapped data with zero copying
type MMapResult struct {
	result        *C.mmap_result_t
	rowCount      int64
	columnCount   int
	columnTypes   []DataType
	columnNames   []string
	closed        bool
	currentRow    int64
	maxFetchSize  int64
	resultOwned   bool // Whether this result owns the result and should free it
	fastRowsCache sync.Pool
	closedFlag    int32 // Atomic flag to safely track closed state
}

// NewMMapResult creates a new MMapResult from a C mmap_result
func NewMMapResult(result *C.mmap_result_t) *MMapResult {
	if result == nil {
		return nil
	}

	// Create the Go wrapper
	mr := &MMapResult{
		result:      result,
		rowCount:    int64(result.row_count),
		columnCount: int(result.column_count),
		columnTypes: make([]DataType, int(result.column_count)),
		columnNames: make([]string, int(result.column_count)),
		resultOwned: true,
		fastRowsCache: sync.Pool{
			New: func() interface{} {
				return &MMapFastRows{}
			},
		},
	}

	// Extract column information
	columnsPtr := unsafe.Pointer(result.columns)
	// Calculate size of mmap_column_meta_t
	columnSize := unsafe.Sizeof(C.mmap_column_meta_t{})

	for i := 0; i < mr.columnCount; i++ {
		// Get column metadata from the array
		colPtr := unsafe.Pointer(uintptr(columnsPtr) + uintptr(i)*columnSize)

		// Extract type (first field)
		typePtr := (*C.mmap_data_type_t)(colPtr)
		mr.columnTypes[i] = DataType(*typePtr)

		// Extract name field - offset to name is 32 bytes (4 uint64_t fields)
		namePtr := (*C.char)(unsafe.Pointer(uintptr(colPtr) + 32))
		mr.columnNames[i] = C.GoString(namePtr)
	}

	// Set up finalizer to automatically free the result when garbage collected
	runtime.SetFinalizer(mr, (*MMapResult).Close)

	return mr
}

// Columns returns the names of the columns in the result
func (mr *MMapResult) Columns() []string {
	return mr.columnNames
}

// ColumnTypes returns the types of the columns in the result
func (mr *MMapResult) ColumnTypes() []DataType {
	return mr.columnTypes
}

// RowCount returns the number of rows in the result
func (mr *MMapResult) RowCount() int64 {
	return mr.rowCount
}

// Close releases the resources associated with the result
func (mr *MMapResult) Close() error {
	// Use atomic compare-and-swap to safely handle concurrent close calls
	if !atomic.CompareAndSwapInt32(&mr.closedFlag, 0, 1) {
		return nil // Already closed
	}

	runtime.SetFinalizer(mr, nil)
	mr.closed = true

	if mr.resultOwned && mr.result != nil {
		C.release_mmap_result(mr.result)
		mr.result = nil
	}

	// Clear references to help GC
	mr.columnNames = nil
	mr.columnTypes = nil

	return nil
}

// FastRows returns a new Rows implementation for this result
// This provides an *extremely* efficient way to access the data
func (mr *MMapResult) FastRows() *MMapFastRows {
	rows := mr.fastRowsCache.Get().(*MMapFastRows)
	rows.Reset(mr)
	return rows
}

// getValue retrieves a value at the given column and row index
// This is a low-level function used by the Rows implementation
func (mr *MMapResult) getValue(colIdx, rowIdx int) (interface{}, bool) {
	if atomic.LoadInt32(&mr.closedFlag) != 0 || mr.result == nil {
		return nil, true // Return nil for closed results
	}

	if colIdx < 0 || colIdx >= mr.columnCount || rowIdx < 0 || rowIdx >= int(mr.rowCount) {
		return nil, true // Out of bounds
	}

	// Calculate size of mmap_column_meta_t
	columnSize := unsafe.Sizeof(C.mmap_column_meta_t{})

	// Get column metadata pointer
	colPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.columns)) + uintptr(colIdx)*columnSize)

	// Extract type (first field)
	typePtr := (*C.mmap_data_type_t)(colPtr)
	colType := DataType(*typePtr)

	// Extract nullmap offset - 3rd uint64_t field (24 bytes offset)
	nullmapOffsetPtr := (*C.uint64_t)(unsafe.Pointer(uintptr(colPtr) + 24))
	nullmapOffset := uint64(*nullmapOffsetPtr)

	// Extract data offset - 1st uint64_t field (8 bytes offset)
	offsetPtr := (*C.uint64_t)(unsafe.Pointer(uintptr(colPtr) + 8))
	dataOffset := uint64(*offsetPtr)

	// Check if value is NULL using the nullmap
	nullmapPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(nullmapOffset))
	nullmap := (*[1 << 30]byte)(nullmapPtr)
	rowByte := rowIdx / 8
	rowBit := rowIdx % 8
	isNull := (nullmap[rowByte] & (1 << byte(rowBit))) != 0

	if isNull {
		return nil, true
	}

	// Get value based on type
	switch colType {
	case TypeBoolean:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]bool)(dataPtr)
		return data[rowIdx], false

	case TypeInt8:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]int8)(dataPtr)
		return data[rowIdx], false

	case TypeInt16:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]int16)(dataPtr)
		return data[rowIdx], false

	case TypeInt32:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]int32)(dataPtr)
		return data[rowIdx], false

	case TypeInt64:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]int64)(dataPtr)
		return data[rowIdx], false

	case TypeUint8:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint8)(dataPtr)
		return data[rowIdx], false

	case TypeUint16:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint16)(dataPtr)
		return data[rowIdx], false

	case TypeUint32:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint32)(dataPtr)
		return data[rowIdx], false

	case TypeUint64:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint64)(dataPtr)
		return data[rowIdx], false

	case TypeFloat:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]float32)(dataPtr)
		return data[rowIdx], false

	case TypeDouble:
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]float64)(dataPtr)
		return data[rowIdx], false

	case TypeVarchar:
		// For VARCHAR, we have an index into the string table
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint64)(dataPtr)
		stringIdx := data[rowIdx]

		if stringIdx == ^uint64(0) { // UINT64_MAX
			return "", false // Empty string
		}

		// Get string from string table
		if stringIdx < uint64(mr.result.string_count) {
			offsetsPtr := unsafe.Pointer(mr.result.string_offsets)
			offsets := (*[1 << 30]uint64)(offsetsPtr)
			offset := offsets[stringIdx]

			// Get string from string table
			strPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.string_table)) + uintptr(offset))
			return C.GoString((*C.char)(strPtr)), false
		}
		return "", false

	case TypeBlob:
		// For BLOB, we have an offset and size
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint64)(dataPtr)
		offset := data[rowIdx*2]
		size := data[rowIdx*2+1]

		if offset == ^uint64(0) { // UINT64_MAX
			return []byte{}, false // Empty blob
		}

		// Create a copy of the blob data
		blobPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.string_table)) + uintptr(offset))
		blob := make([]byte, size)
		copy(blob, (*[1 << 30]byte)(blobPtr)[:size])
		return blob, false

	default:
		// For other types, we have a string
		dataPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.data_buffer)) + uintptr(dataOffset))
		data := (*[1 << 30]uint64)(dataPtr)
		stringIdx := data[rowIdx]

		if stringIdx == ^uint64(0) { // UINT64_MAX
			return "", false // Empty string
		}

		// Get string from string table
		if stringIdx < uint64(mr.result.string_count) {
			offsetsPtr := unsafe.Pointer(mr.result.string_offsets)
			offsets := (*[1 << 30]uint64)(offsetsPtr)
			offset := offsets[stringIdx]

			// Get string from string table
			strPtr := unsafe.Pointer(uintptr(unsafe.Pointer(mr.result.string_table)) + uintptr(offset))
			return C.GoString((*C.char)(strPtr)), false
		}
		return "", false
	}
}

// MMapFastRows implements the driver.Rows interface for MMapResult
// This provides an extremely efficient way to access the data
type MMapFastRows struct {
	mr          *MMapResult
	currentRow  int
	closed      bool
	valueBuffer []driver.Value
	closedFlag  int32 // Atomic flag to safely track closed state
}

// Reset initializes the rows with a new result
func (fr *MMapFastRows) Reset(mr *MMapResult) {
	fr.mr = mr
	fr.currentRow = 0
	fr.closed = false
	atomic.StoreInt32(&fr.closedFlag, 0)

	// Ensure we have enough space in the value buffer
	if fr.valueBuffer == nil || len(fr.valueBuffer) < mr.columnCount {
		fr.valueBuffer = make([]driver.Value, mr.columnCount)
	}
}

// Columns returns the names of the columns
func (fr *MMapFastRows) Columns() []string {
	if atomic.LoadInt32(&fr.closedFlag) != 0 || fr.mr == nil {
		return nil
	}
	return fr.mr.Columns()
}

// ColumnTypeScanType returns the Go type that can be used to scan into
func (fr *MMapFastRows) ColumnTypeScanType(index int) reflect.Type {
	if atomic.LoadInt32(&fr.closedFlag) != 0 || fr.mr == nil || index < 0 || index >= fr.mr.columnCount {
		return nil
	}

	switch fr.mr.columnTypes[index] {
	case TypeBoolean:
		return reflect.TypeOf(bool(false))
	case TypeInt8:
		return reflect.TypeOf(int8(0))
	case TypeInt16:
		return reflect.TypeOf(int16(0))
	case TypeInt32:
		return reflect.TypeOf(int32(0))
	case TypeInt64:
		return reflect.TypeOf(int64(0))
	case TypeUint8:
		return reflect.TypeOf(uint8(0))
	case TypeUint16:
		return reflect.TypeOf(uint16(0))
	case TypeUint32:
		return reflect.TypeOf(uint32(0))
	case TypeUint64:
		return reflect.TypeOf(uint64(0))
	case TypeFloat:
		return reflect.TypeOf(float32(0))
	case TypeDouble:
		return reflect.TypeOf(float64(0))
	case TypeVarchar:
		return reflect.TypeOf(string(""))
	case TypeBlob:
		return reflect.TypeOf([]byte{})
	default:
		return reflect.TypeOf(string(""))
	}
}

// Close closes the rows iterator
func (fr *MMapFastRows) Close() error {
	// Use atomic compare-and-swap to safely handle concurrent close calls
	if !atomic.CompareAndSwapInt32(&fr.closedFlag, 0, 1) {
		return nil // Already closed
	}

	fr.closed = true

	if fr.mr != nil {
		// Return the rows to the pool
		fr.mr.fastRowsCache.Put(fr)
		fr.mr = nil
	}

	return nil
}

// Next moves to the next row
// This is the critical hot path optimized for performance
func (fr *MMapFastRows) Next(dest []driver.Value) error {
	if atomic.LoadInt32(&fr.closedFlag) != 0 || fr.mr == nil {
		return io.EOF
	}

	if fr.currentRow >= int(fr.mr.rowCount) {
		return io.EOF
	}

	// Get all values for this row in one go - this is the most efficient way
	for i := 0; i < fr.mr.columnCount && i < len(dest); i++ {
		value, isNull := fr.mr.getValue(i, fr.currentRow)
		if isNull {
			dest[i] = nil
		} else {
			dest[i] = value
		}
	}

	fr.currentRow++
	return nil
}

// Execute a vectorized query with memory-mapped result
func (conn *Connection) QueryMMap(query string) (*MMapFastRows, error) {
	if conn.conn == nil {
		return nil, errors.New("connection is closed")
	}

	// Prepare query string
	cQuery := cString(query)
	defer freeString(cQuery)

	// Execute the query
	var result C.duckdb_result
	if err := C.duckdb_query(*conn.conn, cQuery, &result); err == C.DuckDBError {
		return nil, errors.New(goString(C.duckdb_result_error(&result)))
	}

	// Create memory-mapped result
	mmapResult := C.create_mmap_result(&result)
	if mmapResult == nil {
		C.duckdb_destroy_result(&result)
		return nil, errors.New("failed to create memory-mapped result")
	}

	// We no longer need the original result
	C.duckdb_destroy_result(&result)

	// Create Go wrapper
	mr := NewMMapResult(mmapResult)

	// Create fast rows
	return mr.FastRows(), nil
}

// Helper to execute with parameters via prepared statement
func (conn *Connection) QueryMMapPrepared(query string, args ...interface{}) (*MMapFastRows, error) {
	// For now, just reuse the simple query version
	// In a real implementation, we would use prepared statements with parameter binding
	return conn.QueryMMap(query)
}
