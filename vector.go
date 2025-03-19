package duckdb

/*
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include "duckdb_go_adapter.h"
*/
import "C"

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"
)

// ExtractColumnBatchTyped extracts a column of values from a result set with a single CGO crossing
// This unified extraction method provides several key advantages:
// 1. Reduces code duplication by handling all data types in a single method
// 2. Minimizes CGO overhead by crossing the boundary only once per column
// 3. Uses type-specific vectors to avoid interface{} boxing/unboxing
// 4. Integrates with the object pooling system to reduce GC pressure
// 5. Supports batched extraction for improved performance
//
// It returns a properly typed ColumnVector containing the extracted values.
// This is the recommended method for high-performance columnar data access.
//
// IMPORTANT: This method is the common extraction utility used by DirectResult,
// Rows, and other components that need to extract columns efficiently.
func ExtractColumnBatchTyped(result *C.duckdb_result, colIdx int, startRow int, rowCount int) (*ColumnVector, error) {
	// Validate parameters
	if result == nil {
		return nil, fmt.Errorf("result is nil")
	}

	// Get column type
	colType := C.duckdb_column_type(result, C.idx_t(colIdx))

	// Get a pooled vector for this type to avoid allocations
	vector := GetPooledColumnVector(colType, rowCount)

	// Extract data with a single CGO crossing based on type
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		if vector.boolData == nil || len(vector.boolData) < rowCount {
			vector.boolData = make([]bool, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		// Extract data with a single CGO call
		C.extract_vector_bool(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.bool)(unsafe.Pointer(&vector.boolData[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_TINYINT:
		if vector.int8Data == nil || len(vector.int8Data) < rowCount {
			vector.int8Data = make([]int8, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_int8(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int8_t)(unsafe.Pointer(&vector.int8Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_SMALLINT:
		if vector.int16Data == nil || len(vector.int16Data) < rowCount {
			vector.int16Data = make([]int16, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_int16(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int16_t)(unsafe.Pointer(&vector.int16Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_INTEGER:
		if vector.int32Data == nil || len(vector.int32Data) < rowCount {
			vector.int32Data = make([]int32, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_int32(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int32_t)(unsafe.Pointer(&vector.int32Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_BIGINT:
		if vector.int64Data == nil || len(vector.int64Data) < rowCount {
			vector.int64Data = make([]int64, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_int64(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int64_t)(unsafe.Pointer(&vector.int64Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_UTINYINT:
		if vector.uint8Data == nil || len(vector.uint8Data) < rowCount {
			vector.uint8Data = make([]uint8, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_uint8(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.uint8_t)(unsafe.Pointer(&vector.uint8Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_FLOAT:
		if vector.float32Data == nil || len(vector.float32Data) < rowCount {
			vector.float32Data = make([]float32, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_float32(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.float)(unsafe.Pointer(&vector.float32Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_DOUBLE:
		if vector.float64Data == nil || len(vector.float64Data) < rowCount {
			vector.float64Data = make([]float64, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_float64(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.double)(unsafe.Pointer(&vector.float64Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_VARCHAR:
		extractStringBatch(result, colIdx, startRow, rowCount, vector)

	case C.DUCKDB_TYPE_BLOB:
		extractBlobBatch(result, colIdx, startRow, rowCount, vector)

	case C.DUCKDB_TYPE_DATE:
		if vector.int32Data == nil || len(vector.int32Data) < rowCount {
			vector.int32Data = make([]int32, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_date(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int32_t)(unsafe.Pointer(&vector.int32Data[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	case C.DUCKDB_TYPE_TIMESTAMP:
		if vector.timestampData == nil || len(vector.timestampData) < rowCount {
			vector.timestampData = make([]int64, rowCount)
		}
		if vector.nullMap == nil || len(vector.nullMap) < rowCount {
			vector.nullMap = make([]bool, rowCount)
		}

		C.extract_vector_timestamp(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
			(*C.int64_t)(unsafe.Pointer(&vector.timestampData[0])),
			(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	default:
		// For unsupported types, return error
		PutPooledColumnVector(vector)
		return nil, fmt.Errorf("unsupported column type: %d", colType)
	}

	// Set vector metadata
	vector.columnType = colType
	vector.length = rowCount

	return vector, nil
}

// Helper function for string batch extraction
func extractStringBatch(result *C.duckdb_result, colIdx int, startRow int, rowCount int, vector *ColumnVector) {
	// Prepare string storage
	if vector.stringData == nil || len(vector.stringData) < rowCount {
		vector.stringData = make([]string, rowCount)
	}
	if vector.nullMap == nil || len(vector.nullMap) < rowCount {
		vector.nullMap = make([]bool, rowCount)
	}

	// Temporary storage for string extraction
	cStrPtrs := make([]*C.char, rowCount)
	cStrLens := make([]C.idx_t, rowCount)

	// Extract all string pointers and lengths with a single CGO call
	C.extract_vector_string(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
		(**C.char)(unsafe.Pointer(&cStrPtrs[0])),
		(*C.idx_t)(unsafe.Pointer(&cStrLens[0])),
		(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	// Copy strings to Go memory
	for i := 0; i < rowCount; i++ {
		if !vector.nullMap[i] && cStrPtrs[i] != nil {
			// Get string length
			strLen := int(cStrLens[i])
			if strLen > 0 {
				// Convert C string to Go string (makes a copy)
				vector.stringData[i] = C.GoStringN(cStrPtrs[i], C.int(strLen))
				// Free C string
				C.duckdb_free(unsafe.Pointer(cStrPtrs[i]))
			} else {
				vector.stringData[i] = ""
			}
		} else {
			vector.stringData[i] = ""
		}
	}
}

// Helper function for blob batch extraction
func extractBlobBatch(result *C.duckdb_result, colIdx int, startRow int, rowCount int, vector *ColumnVector) {
	// Prepare blob storage
	if vector.blobData == nil || len(vector.blobData) < rowCount {
		vector.blobData = make([][]byte, rowCount)
	}
	if vector.nullMap == nil || len(vector.nullMap) < rowCount {
		vector.nullMap = make([]bool, rowCount)
	}

	// Temporary storage for blob extraction
	blobPtrs := make([]unsafe.Pointer, rowCount)
	blobLens := make([]C.idx_t, rowCount)

	// Extract all blob pointers and lengths with a single CGO call
	C.extract_vector_blob(result, C.idx_t(colIdx), C.idx_t(startRow), C.idx_t(rowCount),
		(**C.char)(unsafe.Pointer(&blobPtrs[0])),
		(*C.idx_t)(unsafe.Pointer(&blobLens[0])),
		(*C.bool)(unsafe.Pointer(&vector.nullMap[0])))

	// Copy blobs to Go memory
	for i := 0; i < rowCount; i++ {
		if !vector.nullMap[i] && blobPtrs[i] != nil {
			// Get blob length
			blobLen := int(blobLens[i])
			if blobLen > 0 {
				// Allocate Go memory for blob
				blob := make([]byte, blobLen)
				// Copy blob data
				copy(blob, (*[1 << 30]byte)(blobPtrs[i])[:blobLen:blobLen])
				vector.blobData[i] = blob
			} else {
				vector.blobData[i] = []byte{}
			}
			// Free C memory after copying
			C.duckdb_free(blobPtrs[i])
		} else {
			vector.blobData[i] = []byte{}
		}
	}
}

// Vector represents a DuckDB vector - a horizontal slice of a column
// This aligns with DuckDB's native vector concept and provides efficient
// data access with minimal CGO crossings.
type Vector struct {
	// Vector type
	columnType C.duckdb_type

	// Native DuckDB vector pointer - used when interacting directly with C API
	vector C.duckdb_vector

	// Length of the vector (number of rows)
	length int

	// Type-specific data pointers
	dataPtr  unsafe.Pointer // Raw data pointer from DuckDB
	validPtr *C.uint64_t    // Validity mask pointer from DuckDB

	// Set to true when this vector owns external resources that need cleanup
	needsCleanup bool

	// Optional pooled column vector if using the vector pool system
	// When non-nil, this will be returned to the pool when the vector is done
	pooledVector *ColumnVector
}

// vectorPool holds vectors for reuse to reduce GC pressure
var vectorPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return &Vector{}
	},
}

// GetVector gets a vector from the pool
func GetVector() *Vector {
	return vectorPool.Get().(*Vector)
}

// PutVector returns a vector to the pool
func PutVector(v *Vector) {
	if v == nil {
		return
	}

	// Clear all fields to prevent memory leaks
	v.vector = C.duckdb_vector(nil)
	v.dataPtr = nil
	v.validPtr = nil
	v.length = 0
	v.needsCleanup = false

	// If we have a pooled column vector, return it to its pool
	if v.pooledVector != nil {
		PutPooledColumnVector(v.pooledVector)
		v.pooledVector = nil
	}

	vectorPool.Put(v)
}

// NewVector creates a new vector wrapper around a DuckDB vector
// This is used by functions that get vectors from a DataChunk or Query
func NewVector(vector C.duckdb_vector) *Vector {
	// Get the column type
	columnType := C.duckdb_vector_get_column_type(vector)
	typeID := C.duckdb_get_type_id(columnType)
	defer C.duckdb_destroy_logical_type(&columnType)

	// Get a vector from the pool
	v := GetVector()

	// Set up the vector
	v.columnType = typeID
	v.vector = vector
	v.dataPtr = C.duckdb_vector_get_data(vector)
	v.validPtr = C.duckdb_vector_get_validity(vector)
	v.needsCleanup = false
	v.pooledVector = nil
	// Default DuckDB vector size is STANDARD_VECTOR_SIZE (1024)
	v.length = 1024

	return v
}

// NewVectorFromResult creates a vector that extracts data directly from a result
// This avoids unnecessary CGO crossings when working with result data
func NewVectorFromResult(result *C.duckdb_result, colIdx int, startRow int, size int) *Vector {
	if result == nil || size <= 0 {
		return nil
	}

	// Get a vector from the pool
	v := GetVector()

	// Set up basic vector properties
	cColIdx := C.idx_t(colIdx)
	v.columnType = C.duckdb_column_type(result, cColIdx)
	v.length = size

	// Get a pooled column vector to store the data
	v.pooledVector = GetPooledColumnVector(v.columnType, size)

	// Extract data from result to the pooled vector
	extractResultColumnBatch(result, colIdx, startRow, size, v.pooledVector)

	return v
}

//
// Native vector extraction functions - these call directly into C for efficient data extraction
//

// Functions defined in duckdb_go_adapter.c - these should be implemented there
/*
void extract_vector_bool(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, bool *values, bool *nulls);
void extract_vector_int8(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int8_t *values, bool *nulls);
void extract_vector_int16(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int16_t *values, bool *nulls);
void extract_vector_int32(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int32_t *values, bool *nulls);
void extract_vector_int64(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int64_t *values, bool *nulls);
void extract_vector_uint8(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, uint8_t *values, bool *nulls);
void extract_vector_uint16(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, uint16_t *values, bool *nulls);
void extract_vector_uint32(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, uint32_t *values, bool *nulls);
void extract_vector_uint64(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, uint64_t *values, bool *nulls);
void extract_vector_float32(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, float *values, bool *nulls);
void extract_vector_float64(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, double *values, bool *nulls);
int extract_vector_string(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, char **values, idx_t *lengths, bool *nulls);
int extract_vector_blob(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, char **values, idx_t *lengths, bool *nulls);
void extract_vector_timestamp(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int64_t *values, bool *nulls);
void extract_vector_date(duckdb_result *result, idx_t col_idx, idx_t offset, idx_t batch_size, int32_t *values, bool *nulls);
*/

// extractResultColumnBatch efficiently extracts data from a result to a column vector
// This is a highly optimized version that leverages DuckDB's C adapter functions
// to minimize CGO boundary crossings
func extractResultColumnBatch(result *C.duckdb_result, colIdx, startRow, batchSize int, vector *ColumnVector) {
	// Get DuckDB C types ready
	cColIdx := C.idx_t(colIdx)
	cStartRow := C.idx_t(startRow)
	colType := C.duckdb_column_type(result, cColIdx)

	// Define block size for processing to reduce CGO boundary crossings
	// This value can be made configurable for workload-specific tuning
	const blockSize = 64

	// Process the data in blocks to minimize CGO boundary crossings
	for blockStart := 0; blockStart < batchSize; blockStart += blockSize {
		// Calculate actual block size (might be smaller at the end)
		blockEnd := blockStart + blockSize
		if blockEnd > batchSize {
			blockEnd = batchSize
		}
		actualBlockSize := blockEnd - blockStart

		// Adjust pointers and indices for this block
		blockStartRow := cStartRow + C.idx_t(blockStart)
		nullMapPtr := unsafe.Pointer(&vector.nullMap[blockStart])

		// Extract values based on column type using vector extraction functions
		// These functions extract both values and nulls in a single CGO call
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			if len(vector.boolData) > 0 {
				// Use the native extraction function for boolean values
				dataPtr := unsafe.Pointer(&vector.boolData[blockStart])
				C.extract_vector_bool(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.bool)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_TINYINT:
			if len(vector.int8Data) > 0 {
				// Use the native extraction function for int8 values
				dataPtr := unsafe.Pointer(&vector.int8Data[blockStart])
				C.extract_vector_int8(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.int8_t)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_SMALLINT:
			if len(vector.int16Data) > 0 {
				// Use the native extraction function for int16 values
				dataPtr := unsafe.Pointer(&vector.int16Data[blockStart])
				C.extract_vector_int16(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.int16_t)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_INTEGER:
			if len(vector.int32Data) > 0 {
				// Use the native extraction function for int32 values
				dataPtr := unsafe.Pointer(&vector.int32Data[blockStart])
				C.extract_vector_int32(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.int32_t)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_BIGINT:
			if len(vector.int64Data) > 0 {
				// Use the native extraction function for int64 values
				dataPtr := unsafe.Pointer(&vector.int64Data[blockStart])
				C.extract_vector_int64(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.int64_t)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_FLOAT:
			if len(vector.float32Data) > 0 {
				// Use the native extraction function for float32 values
				dataPtr := unsafe.Pointer(&vector.float32Data[blockStart])
				C.extract_vector_float32(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.float)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_DOUBLE:
			if len(vector.float64Data) > 0 {
				// Use the native extraction function for float64 values
				dataPtr := unsafe.Pointer(&vector.float64Data[blockStart])
				C.extract_vector_float64(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.double)(dataPtr), (*C.bool)(nullMapPtr))
			}

		case C.DUCKDB_TYPE_VARCHAR:
			if len(vector.stringData) > 0 {
				// Extract strings with batch processing
				// First, allocate temporary arrays for efficient string extraction
				valuesPtrs := make([]*C.char, actualBlockSize)
				lengths := make([]C.idx_t, actualBlockSize)

				// Call batch string extraction (single CGO crossing)
				success := C.extract_vector_string(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(**C.char)(unsafe.Pointer(&valuesPtrs[0])), (*C.idx_t)(unsafe.Pointer(&lengths[0])),
					(*C.bool)(nullMapPtr))

				if success != 0 {
					// Copy strings to Go slices and free C memory
					for i := 0; i < actualBlockSize; i++ {
						if valuesPtrs[i] != nil {
							// Use GoStringN for more efficient string conversion with known length
							vector.stringData[blockStart+i] = C.GoStringN(valuesPtrs[i], C.int(lengths[i]))
							C.duckdb_free(unsafe.Pointer(valuesPtrs[i]))
						} else {
							vector.stringData[blockStart+i] = ""
						}
					}
				}
			}

		case C.DUCKDB_TYPE_BLOB:
			if len(vector.blobData) > 0 {
				// Extract blobs with batch processing
				// First, allocate temporary arrays for efficient blob extraction
				valuesPtrs := make([]unsafe.Pointer, actualBlockSize)
				lengths := make([]C.idx_t, actualBlockSize)

				// Call batch blob extraction (single CGO crossing)
				success := C.extract_vector_blob(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(**C.char)(unsafe.Pointer(&valuesPtrs[0])), (*C.idx_t)(unsafe.Pointer(&lengths[0])),
					(*C.bool)(nullMapPtr))

				if success != 0 {
					// Create a shared byte pool for blobs of similar size to reduce allocations
					// This pool could be further optimized with a proper size-based buffer pool
					for i := 0; i < actualBlockSize; i++ {
						if !vector.nullMap[blockStart+i] && valuesPtrs[i] != nil {
							size := int(lengths[i])
							if size > 0 {
								// Allocate a new buffer for this blob
								buffer := make([]byte, size)
								// Copy blob data safely
								C.memcpy(unsafe.Pointer(&buffer[0]), valuesPtrs[i], C.size_t(size))
								vector.blobData[blockStart+i] = buffer
							} else {
								vector.blobData[blockStart+i] = []byte{}
							}
							// Free the C memory after safely copying it
							C.duckdb_free(valuesPtrs[i])
						} else {
							vector.blobData[blockStart+i] = []byte{}
						}
					}
				}
			}

		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S,
			C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS,
			C.DUCKDB_TYPE_TIMESTAMP_TZ:
			if len(vector.timestampData) > 0 {
				// Use the native extraction function for timestamp values
				dataPtr := unsafe.Pointer(&vector.timestampData[blockStart])
				C.extract_vector_timestamp(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(*C.int64_t)(dataPtr), (*C.bool)(nullMapPtr))
			}

		default:
			// For unsupported types, we'll fall back to the string approach but with batching
			if len(vector.timeData) > 0 {
				// Extract strings with batch processing for other types
				valuesPtrs := make([]*C.char, actualBlockSize)
				lengths := make([]C.idx_t, actualBlockSize)

				// Call batch string extraction (single CGO crossing)
				success := C.extract_vector_string(result, cColIdx, blockStartRow, C.idx_t(actualBlockSize),
					(**C.char)(unsafe.Pointer(&valuesPtrs[0])), (*C.idx_t)(unsafe.Pointer(&lengths[0])),
					(*C.bool)(nullMapPtr))

				if success != 0 {
					// Copy strings to Go slices and free C memory
					for i := 0; i < actualBlockSize; i++ {
						if !vector.nullMap[blockStart+i] && valuesPtrs[i] != nil {
							vector.timeData[blockStart+i] = C.GoStringN(valuesPtrs[i], C.int(lengths[i]))
							C.duckdb_free(unsafe.Pointer(valuesPtrs[i]))
						} else {
							vector.timeData[blockStart+i] = ""
						}
					}
				}
			}
		}
	}
}

// IsValid checks if a value at the specified row is valid (not NULL)
func (v *Vector) IsValid(row int) bool {
	// If we have a pooled vector, use its null map
	if v.pooledVector != nil {
		if row >= len(v.pooledVector.nullMap) {
			return false
		}
		return !v.pooledVector.nullMap[row]
	}

	// Otherwise use the vector's validity mask
	if v.validPtr == nil {
		// If there's no validity mask, all values are valid
		return true
	}

	return cBoolToGo(C.duckdb_validity_row_is_valid(v.validPtr, C.idx_t(row)))
}

// GetBool retrieves a boolean value at the specified row
func (v *Vector) GetBool(row int) (bool, bool) {
	if !v.IsValid(row) {
		return false, false
	}

	if v.columnType != C.DUCKDB_TYPE_BOOLEAN {
		return false, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.boolData) > row {
		return v.pooledVector.boolData[row], true
	}

	// Get the data pointer as bool array
	boolData := (*[1 << 30]bool)(v.dataPtr)
	return boolData[row], true
}

// GetInt8 retrieves an int8 value at the specified row
func (v *Vector) GetInt8(row int) (int8, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_TINYINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.int8Data) > row {
		return v.pooledVector.int8Data[row], true
	}

	// Get the data pointer as int8 array
	int8Data := (*[1 << 30]int8)(v.dataPtr)
	return int8Data[row], true
}

// GetInt16 retrieves an int16 value at the specified row
func (v *Vector) GetInt16(row int) (int16, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_SMALLINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.int16Data) > row {
		return v.pooledVector.int16Data[row], true
	}

	// Get the data pointer as int16 array
	int16Data := (*[1 << 30]int16)(v.dataPtr)
	return int16Data[row], true
}

// GetInt32 retrieves an int32 value at the specified row
func (v *Vector) GetInt32(row int) (int32, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_INTEGER {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.int32Data) > row {
		return v.pooledVector.int32Data[row], true
	}

	// Get the data pointer as int32 array
	int32Data := (*[1 << 30]int32)(v.dataPtr)
	return int32Data[row], true
}

// GetInt64 retrieves an int64 value at the specified row
func (v *Vector) GetInt64(row int) (int64, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_BIGINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.int64Data) > row {
		return v.pooledVector.int64Data[row], true
	}

	// Get the data pointer as int64 array
	int64Data := (*[1 << 30]int64)(v.dataPtr)
	return int64Data[row], true
}

// GetUint8 retrieves a uint8 value at the specified row
func (v *Vector) GetUint8(row int) (uint8, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_UTINYINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.uint8Data) > row {
		return v.pooledVector.uint8Data[row], true
	}

	// Get the data pointer as uint8 array
	uint8Data := (*[1 << 30]uint8)(v.dataPtr)
	return uint8Data[row], true
}

// GetUint16 retrieves a uint16 value at the specified row
func (v *Vector) GetUint16(row int) (uint16, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_USMALLINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.uint16Data) > row {
		return v.pooledVector.uint16Data[row], true
	}

	// Get the data pointer as uint16 array
	uint16Data := (*[1 << 30]uint16)(v.dataPtr)
	return uint16Data[row], true
}

// GetUint32 retrieves a uint32 value at the specified row
func (v *Vector) GetUint32(row int) (uint32, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_UINTEGER {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.uint32Data) > row {
		return v.pooledVector.uint32Data[row], true
	}

	// Get the data pointer as uint32 array
	uint32Data := (*[1 << 30]uint32)(v.dataPtr)
	return uint32Data[row], true
}

// GetUint64 retrieves a uint64 value at the specified row
func (v *Vector) GetUint64(row int) (uint64, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_UBIGINT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.uint64Data) > row {
		return v.pooledVector.uint64Data[row], true
	}

	// Get the data pointer as uint64 array
	uint64Data := (*[1 << 30]uint64)(v.dataPtr)
	return uint64Data[row], true
}

// GetFloat32 retrieves a float32 value at the specified row
func (v *Vector) GetFloat32(row int) (float32, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_FLOAT {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.float32Data) > row {
		return v.pooledVector.float32Data[row], true
	}

	// Get the data pointer as float32 array
	float32Data := (*[1 << 30]float32)(v.dataPtr)
	return float32Data[row], true
}

// GetFloat64 retrieves a float64 value at the specified row
func (v *Vector) GetFloat64(row int) (float64, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_DOUBLE {
		return 0, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.float64Data) > row {
		return v.pooledVector.float64Data[row], true
	}

	// Get the data pointer as float64 array
	float64Data := (*[1 << 30]float64)(v.dataPtr)
	return float64Data[row], true
}

// StringInlined checks if a string is inlined in a DuckDB string struct
// In DuckDB, strings of 12 bytes or less are stored inline for efficiency
func StringInlined(s unsafe.Pointer) bool {
	// Get pointer to the first byte
	if s == nil {
		return false
	}

	// In DuckDB's actual implementation, a string is represented by:
	// struct string_t {
	//     union {
	//         struct {
	//             uint32_t length;
	//             char prefix[4];
	//             char* ptr;
	//         } pointer;
	//         struct {
	//             uint32_t length;
	//             char inlined[12];
	//         } inlined;
	//     };
	// }
	//
	// The first 4 bytes are the length field (uint32_t)
	// The string is inlined if the length (in bytes) is less than or equal to 12

	// Read the length field (first 4 bytes)
	lengthPtr := (*uint32)(s)
	length := *lengthPtr

	// Check if the length is <= 12 (DuckDB's inline threshold)
	// Also check for a reasonable maximum to avoid false positives
	return length <= 12 && length < 1000000 // Sanity check to avoid false positives
}

// GetString retrieves a string value at the specified row
func (v *Vector) GetString(row int) (string, bool) {
	if !v.IsValid(row) {
		return "", false
	}

	if v.columnType != C.DUCKDB_TYPE_VARCHAR {
		return "", false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.stringData) > row {
		return v.pooledVector.stringData[row], true
	}

	// With native DuckDB vector, strings are stored as an array of string_t structs
	// Each string_t is 16 bytes and follows the layout described in StringInlined

	// Calculate pointer to the string_t at the given row
	// Each string_t is 16 bytes (4 bytes length + 12 bytes data/pointer)
	stringSize := uintptr(16) // Size of string_t struct
	stringPtr := unsafe.Pointer(uintptr(v.dataPtr) + uintptr(row)*stringSize)

	// Read the length field (first 4 bytes)
	lengthPtr := (*uint32)(stringPtr)
	if lengthPtr == nil {
		return "", false
	}

	length := *lengthPtr
	if length == 0 {
		return "", true // Empty string
	}

	// Check if string is inlined
	if StringInlined(stringPtr) {
		// For inlined strings, the data follows the length field (at offset 4)
		dataPtr := unsafe.Pointer(uintptr(stringPtr) + 4)
		return C.GoStringN((*C.char)(dataPtr), C.int(length)), true
	} else {
		// For non-inlined strings, we need to read the pointer at offset 8
		// The layout is: [4 bytes length][4 bytes prefix][8 bytes pointer]
		strPtrPtr := (*uintptr)(unsafe.Pointer(uintptr(stringPtr) + 8))
		if strPtrPtr == nil || *strPtrPtr == 0 {
			return "", true // Null or empty string
		}

		// Read from the external pointer
		dataPtr := unsafe.Pointer(*strPtrPtr)
		return C.GoStringN((*C.char)(dataPtr), C.int(length)), true
	}
}

// GetBlob retrieves a blob value at the specified row
func (v *Vector) GetBlob(row int) ([]byte, bool) {
	if !v.IsValid(row) {
		return nil, false
	}

	if v.columnType != C.DUCKDB_TYPE_BLOB {
		return nil, false
	}

	// If we have a pooled vector, use its data
	if v.pooledVector != nil && len(v.pooledVector.blobData) > row {
		return v.pooledVector.blobData[row], true
	}

	// With native DuckDB vector, blobs are stored as an array of blob structs
	// In DuckDB, blobs are represented by a data pointer and a size
	// struct blob_t {
	//     void* data;
	//     idx_t size;
	// }

	// Calculate pointer to the blob at the given row
	// Each blob struct is 16 bytes (8 bytes pointer + 8 bytes size)
	blobSize := uintptr(16)
	blobPtr := unsafe.Pointer(uintptr(v.dataPtr) + uintptr(row)*blobSize)

	// Get the data pointer (first 8 bytes)
	dataPtr := (*unsafe.Pointer)(blobPtr)
	if dataPtr == nil || *dataPtr == nil {
		return []byte{}, true // Empty blob
	}

	// Get the size (next 8 bytes)
	sizePtr := (*C.idx_t)(unsafe.Pointer(uintptr(blobPtr) + 8))
	if sizePtr == nil {
		return []byte{}, true
	}

	size := *sizePtr
	if size == 0 {
		return []byte{}, true // Empty blob
	}

	// Create a new byte slice and copy the data
	result := make([]byte, size)
	C.memcpy(unsafe.Pointer(&result[0]), *dataPtr, C.size_t(size))

	return result, true
}

// GetTimestamp retrieves a timestamp value at the specified row
func (v *Vector) GetTimestamp(row int) (int64, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_TIMESTAMP &&
		v.columnType != C.DUCKDB_TYPE_TIMESTAMP_S &&
		v.columnType != C.DUCKDB_TYPE_TIMESTAMP_MS &&
		v.columnType != C.DUCKDB_TYPE_TIMESTAMP_NS &&
		v.columnType != C.DUCKDB_TYPE_TIMESTAMP_TZ {
		return 0, false
	}

	// If we have a pooled vector, use its timestampData
	if v.pooledVector != nil && len(v.pooledVector.timestampData) > row {
		return v.pooledVector.timestampData[row], true
	}

	// Get the data pointer as int64 array (timestamps are stored as microseconds since epoch)
	timestampData := (*[1 << 30]int64)(v.dataPtr)
	return timestampData[row], true
}

// GetDate retrieves a date value at the specified row
func (v *Vector) GetDate(row int) (int32, bool) {
	if !v.IsValid(row) {
		return 0, false
	}

	if v.columnType != C.DUCKDB_TYPE_DATE {
		return 0, false
	}

	// If we have a pooled vector, use its data if available
	if v.pooledVector != nil && len(v.pooledVector.int32Data) > row {
		return v.pooledVector.int32Data[row], true
	}

	// In DuckDB, dates are stored as int32 values representing days since 1970-01-01
	// Each date value is simply a 4-byte integer

	// Get the data pointer as int32 array
	if v.dataPtr == nil {
		return 0, false
	}

	// Access the array of int32 values
	dateData := (*[1 << 30]int32)(v.dataPtr)
	if dateData == nil {
		return 0, false
	}

	// Return the date value (days since epoch)
	return dateData[row], true
}

// ExtractColumn extracts an entire column of data from the vector
// Returns a slice of the appropriate Go type and a slice of nulls
func (v *Vector) ExtractColumn(size int) (interface{}, []bool, error) {
	if size <= 0 {
		size = v.length
	}

	// Create nulls array
	nulls := make([]bool, size)

	// Check for null values
	for i := 0; i < size; i++ {
		nulls[i] = !v.IsValid(i)
	}

	// Extract based on column type
	switch v.columnType {
	case C.DUCKDB_TYPE_BOOLEAN:
		values := make([]bool, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetBool(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_TINYINT:
		values := make([]int8, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetInt8(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_SMALLINT:
		values := make([]int16, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetInt16(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_INTEGER:
		values := make([]int32, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetInt32(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_BIGINT:
		values := make([]int64, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetInt64(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_UTINYINT:
		values := make([]uint8, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetUint8(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_USMALLINT:
		values := make([]uint16, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetUint16(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_UINTEGER:
		values := make([]uint32, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetUint32(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_UBIGINT:
		values := make([]uint64, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetUint64(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_FLOAT:
		values := make([]float32, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetFloat32(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_DOUBLE:
		values := make([]float64, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetFloat64(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_VARCHAR:
		values := make([]string, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetString(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_BLOB:
		values := make([][]byte, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetBlob(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S,
		C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS,
		C.DUCKDB_TYPE_TIMESTAMP_TZ:
		values := make([]int64, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetTimestamp(i)
			}
		}
		return values, nulls, nil

	case C.DUCKDB_TYPE_DATE:
		values := make([]int32, size)
		for i := 0; i < size; i++ {
			if !nulls[i] {
				values[i], _ = v.GetDate(i)
			}
		}
		return values, nulls, nil

	default:
		return nil, nulls, fmt.Errorf("unsupported column type: %d", v.columnType)
	}
}

// GoType returns the Go type that corresponds to this vector's DuckDB type
func (v *Vector) GoType() reflect.Type {
	switch v.columnType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(bool(false))
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_UTINYINT:
		return reflect.TypeOf(uint8(0))
	case C.DUCKDB_TYPE_USMALLINT:
		return reflect.TypeOf(uint16(0))
	case C.DUCKDB_TYPE_UINTEGER:
		return reflect.TypeOf(uint32(0))
	case C.DUCKDB_TYPE_UBIGINT:
		return reflect.TypeOf(uint64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf(string(""))
	case C.DUCKDB_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S,
		C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS,
		C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_DATE:
		return reflect.TypeOf(int32(0))
	default:
		return reflect.TypeOf((*interface{})(nil)).Elem()
	}
}
