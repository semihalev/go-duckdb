#ifndef MMAP_ADAPTER_H
#define MMAP_ADAPTER_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <duckdb.h>

// Data type identifiers
typedef enum {
    MMAP_TYPE_NULL = 0,
    MMAP_TYPE_BOOLEAN = 1,
    MMAP_TYPE_INT8 = 2,
    MMAP_TYPE_INT16 = 3,
    MMAP_TYPE_INT32 = 4,
    MMAP_TYPE_INT64 = 5,
    MMAP_TYPE_UINT8 = 6,
    MMAP_TYPE_UINT16 = 7,
    MMAP_TYPE_UINT32 = 8,
    MMAP_TYPE_UINT64 = 9,
    MMAP_TYPE_FLOAT = 10,
    MMAP_TYPE_DOUBLE = 11,
    MMAP_TYPE_VARCHAR = 12,
    MMAP_TYPE_BLOB = 13,
    MMAP_TYPE_DATE = 14,
    MMAP_TYPE_TIME = 15,
    MMAP_TYPE_TIMESTAMP = 16,
    MMAP_TYPE_INTERVAL = 17,
    MMAP_TYPE_HUGEINT = 18,
    MMAP_TYPE_DECIMAL = 19,
    MMAP_TYPE_UUID = 20,
    MMAP_TYPE_STRUCT = 21,
    MMAP_TYPE_LIST = 22,
    MMAP_TYPE_MAP = 23,
    MMAP_TYPE_ENUM = 24,
    MMAP_TYPE_BIT = 25,
} mmap_data_type_t;

// Metadata header for column description
typedef struct {
    // Column metadata
    mmap_data_type_t type;     // Data type
    uint64_t offset;          // Offset within data buffer
    uint64_t size;            // Size of column data
    uint64_t nullmap_offset;  // Offset of null bitmap
    
    // Column properties
    char name[128];           // Column name
    bool is_fixed_width;      // Whether the type has fixed width
    uint32_t element_size;    // Size of element for fixed-width types
} mmap_column_meta_t;

// Result block for fast interprocess data transfer
typedef struct {
    // Control information
    uint64_t ref_count;        // Reference counter for memory management
    uint64_t row_count;        // Number of rows in the result
    uint64_t column_count;     // Number of columns
    
    // Result data
    mmap_column_meta_t *columns;    // Array of column metadata
    void *data_buffer;              // Raw data buffer for all columns
    uint64_t data_size;             // Size of data buffer
    
    // String table for deduplication
    char *string_table;        // String data
    uint64_t *string_offsets;  // Offsets into string table
    uint64_t string_count;     // Number of unique strings
    uint64_t string_data_size; // Total size of string data
} mmap_result_t;

// Create a new memory-mapped result from a DuckDB result
mmap_result_t* create_mmap_result(duckdb_result *result);

// Extract all data from a DuckDB result into the memory-mapped structure
// This is the key optimization - one CGO call extracts all data
void extract_all_data(mmap_result_t *mmap_result, duckdb_result *result);

// Release a memory-mapped result
void free_mmap_result(mmap_result_t *mmap_result);

// Increase reference count
void retain_mmap_result(mmap_result_t *mmap_result);

// Decrease reference count and free if zero
void release_mmap_result(mmap_result_t *mmap_result);

// Get duckdb data type from result
mmap_data_type_t get_mmap_data_type(duckdb_type type);

#endif // MMAP_ADAPTER_H