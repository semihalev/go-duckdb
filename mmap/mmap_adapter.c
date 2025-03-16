#include "mmap_adapter.h"

// Convert DuckDB type to our internal data type
mmap_data_type_t get_mmap_data_type(duckdb_type type) {
    switch (type) {
        case DUCKDB_TYPE_BOOLEAN:
            return MMAP_TYPE_BOOLEAN;
        case DUCKDB_TYPE_TINYINT:
            return MMAP_TYPE_INT8;
        case DUCKDB_TYPE_SMALLINT:
            return MMAP_TYPE_INT16;
        case DUCKDB_TYPE_INTEGER:
            return MMAP_TYPE_INT32;
        case DUCKDB_TYPE_BIGINT:
            return MMAP_TYPE_INT64;
        case DUCKDB_TYPE_UTINYINT:
            return MMAP_TYPE_UINT8;
        case DUCKDB_TYPE_USMALLINT:
            return MMAP_TYPE_UINT16;
        case DUCKDB_TYPE_UINTEGER:
            return MMAP_TYPE_UINT32;
        case DUCKDB_TYPE_UBIGINT:
            return MMAP_TYPE_UINT64;
        case DUCKDB_TYPE_FLOAT:
            return MMAP_TYPE_FLOAT;
        case DUCKDB_TYPE_DOUBLE:
            return MMAP_TYPE_DOUBLE;
        case DUCKDB_TYPE_VARCHAR:
            return MMAP_TYPE_VARCHAR;
        case DUCKDB_TYPE_BLOB:
            return MMAP_TYPE_BLOB;
        case DUCKDB_TYPE_DATE:
            return MMAP_TYPE_DATE;
        case DUCKDB_TYPE_TIME:
            return MMAP_TYPE_TIME;
        case DUCKDB_TYPE_TIMESTAMP:
            return MMAP_TYPE_TIMESTAMP;
        case DUCKDB_TYPE_INTERVAL:
            return MMAP_TYPE_INTERVAL;
        case DUCKDB_TYPE_HUGEINT:
            return MMAP_TYPE_HUGEINT;
        case DUCKDB_TYPE_DECIMAL:
            return MMAP_TYPE_DECIMAL;
        case DUCKDB_TYPE_UUID:
            return MMAP_TYPE_UUID;
        case DUCKDB_TYPE_STRUCT:
            return MMAP_TYPE_STRUCT;
        case DUCKDB_TYPE_LIST:
            return MMAP_TYPE_LIST;
        case DUCKDB_TYPE_MAP:
            return MMAP_TYPE_MAP;
        case DUCKDB_TYPE_ENUM:
            return MMAP_TYPE_ENUM;
        case DUCKDB_TYPE_BIT:
            return MMAP_TYPE_BIT;
        default:
            return MMAP_TYPE_VARCHAR; // Default to string for unknown types
    }
}

// Create a memory-mapped result structure
mmap_result_t* create_mmap_result(duckdb_result *result) {
    if (!result) {
        return NULL;
    }
    
    // Allocate result structure
    mmap_result_t *mmap_result = (mmap_result_t*)malloc(sizeof(mmap_result_t));
    if (!mmap_result) {
        return NULL;
    }
    
    // Initialize counters
    mmap_result->ref_count = 1;
    mmap_result->row_count = duckdb_row_count(result);
    mmap_result->column_count = duckdb_column_count(result);
    
    // Allocate column metadata array
    mmap_result->columns = (mmap_column_meta_t*)malloc(sizeof(mmap_column_meta_t) * mmap_result->column_count);
    if (!mmap_result->columns) {
        free(mmap_result);
        return NULL;
    }
    
    // Initial pass to calculate buffer sizes and set up metadata
    uint64_t total_data_size = 0;
    uint64_t string_data_size = 0;
    uint64_t string_count = 0;
    
    // First pass: analyze data types and calculate buffer sizes
    for (idx_t col = 0; col < mmap_result->column_count; col++) {
        mmap_column_meta_t *col_meta = &mmap_result->columns[col];
        
        // Get column name and type
        const char *col_name = duckdb_column_name(result, col);
        duckdb_type col_type = duckdb_column_type(result, col);
        
        // Set column metadata
        strncpy(col_meta->name, col_name, sizeof(col_meta->name) - 1);
        col_meta->name[sizeof(col_meta->name) - 1] = '\0';
        col_meta->type = get_mmap_data_type(col_type);
        
        // Calculate nullmap size (rounded up to bytes)
        uint64_t nullmap_size = (mmap_result->row_count + 7) / 8;
        
        // Set the nullmap offset and update total data size
        col_meta->nullmap_offset = total_data_size;
        total_data_size += nullmap_size;
        
        // Calculate the size based on data type
        switch (col_meta->type) {
            case MMAP_TYPE_BOOLEAN:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(bool);
                col_meta->size = mmap_result->row_count * sizeof(bool);
                break;
                
            case MMAP_TYPE_INT8:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(int8_t);
                col_meta->size = mmap_result->row_count * sizeof(int8_t);
                break;
                
            case MMAP_TYPE_INT16:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(int16_t);
                col_meta->size = mmap_result->row_count * sizeof(int16_t);
                break;
                
            case MMAP_TYPE_INT32:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(int32_t);
                col_meta->size = mmap_result->row_count * sizeof(int32_t);
                break;
                
            case MMAP_TYPE_INT64:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(int64_t);
                col_meta->size = mmap_result->row_count * sizeof(int64_t);
                break;
                
            case MMAP_TYPE_UINT8:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint8_t);
                col_meta->size = mmap_result->row_count * sizeof(uint8_t);
                break;
                
            case MMAP_TYPE_UINT16:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint16_t);
                col_meta->size = mmap_result->row_count * sizeof(uint16_t);
                break;
                
            case MMAP_TYPE_UINT32:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint32_t);
                col_meta->size = mmap_result->row_count * sizeof(uint32_t);
                break;
                
            case MMAP_TYPE_UINT64:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint64_t);
                col_meta->size = mmap_result->row_count * sizeof(uint64_t);
                break;
                
            case MMAP_TYPE_FLOAT:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(float);
                col_meta->size = mmap_result->row_count * sizeof(float);
                break;
                
            case MMAP_TYPE_DOUBLE:
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(double);
                col_meta->size = mmap_result->row_count * sizeof(double);
                break;
                
            case MMAP_TYPE_VARCHAR:
                // For strings, we store string table indexes (fixed-width)
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint64_t);  // Index into string table
                col_meta->size = mmap_result->row_count * sizeof(uint64_t);
                
                // First pass, estimate string data size based on column avg
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    if (!duckdb_value_is_null(result, col, row)) {
                        char *str = duckdb_value_varchar(result, col, row);
                        if (str) {
                            string_data_size += strlen(str) + 1;  // +1 for null terminator
                            string_count++;
                            duckdb_free(str);
                        }
                    }
                }
                break;
                
            case MMAP_TYPE_BLOB:
                // For blobs, we store (offset, size) pairs (fixed-width)
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint64_t) * 2;  // Offset + Size 
                col_meta->size = mmap_result->row_count * sizeof(uint64_t) * 2;
                
                // Estimate blob data size
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    if (!duckdb_value_is_null(result, col, row)) {
                        duckdb_blob blob = duckdb_value_blob(result, col, row);
                        string_data_size += blob.size;
                        if (blob.data) {
                            duckdb_free(blob.data);
                        }
                    }
                }
                break;
                
            default:
                // For other types, convert to strings for now
                col_meta->is_fixed_width = true;
                col_meta->element_size = sizeof(uint64_t);  // Index into string table
                col_meta->size = mmap_result->row_count * sizeof(uint64_t);
                
                // Estimate string data size
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    if (!duckdb_value_is_null(result, col, row)) {
                        char *str = duckdb_value_varchar(result, col, row);
                        if (str) {
                            string_data_size += strlen(str) + 1;
                            string_count++;
                            duckdb_free(str);
                        }
                    }
                }
                break;
        }
        
        // Set the data offset and update total size
        col_meta->offset = total_data_size;
        total_data_size += col_meta->size;
    }
    
    // Allocate data buffer
    mmap_result->data_size = total_data_size;
    mmap_result->data_buffer = calloc(1, total_data_size);
    if (!mmap_result->data_buffer) {
        free(mmap_result->columns);
        free(mmap_result);
        return NULL;
    }
    
    // Allocate string table if needed
    mmap_result->string_count = string_count;
    mmap_result->string_data_size = string_data_size;
    
    if (string_count > 0) {
        mmap_result->string_table = (char*)malloc(string_data_size);
        mmap_result->string_offsets = (uint64_t*)malloc(string_count * sizeof(uint64_t));
        
        if (!mmap_result->string_table || !mmap_result->string_offsets) {
            if (mmap_result->string_table) free(mmap_result->string_table);
            if (mmap_result->string_offsets) free(mmap_result->string_offsets);
            free(mmap_result->data_buffer);
            free(mmap_result->columns);
            free(mmap_result);
            return NULL;
        }
    } else {
        mmap_result->string_table = NULL;
        mmap_result->string_offsets = NULL;
    }
    
    // Extract all data - this is the core optimization
    extract_all_data(mmap_result, result);
    
    return mmap_result;
}

// THE KEY OPTIMIZATION: Extract all data from a DuckDB result in a single batch
void extract_all_data(mmap_result_t *mmap_result, duckdb_result *result) {
    // Initialize string table counters
    uint64_t next_string_idx = 0;
    uint64_t next_string_offset = 0;

    // Process all columns and rows in a single pass
    for (idx_t col = 0; col < mmap_result->column_count; col++) {
        mmap_column_meta_t *col_meta = &mmap_result->columns[col];
        
        // Process nullity bitmap
        uint8_t *nullmap = (uint8_t*)((char*)mmap_result->data_buffer + col_meta->nullmap_offset);
        
        // Process column data based on type
        switch (col_meta->type) {
            case MMAP_TYPE_BOOLEAN: {
                bool *data = (bool*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    // Set nullity bit
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_boolean(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_INT8: {
                int8_t *data = (int8_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_int8(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_INT16: {
                int16_t *data = (int16_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_int16(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_INT32: {
                int32_t *data = (int32_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_int32(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_INT64: {
                int64_t *data = (int64_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_int64(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_UINT8: {
                uint8_t *data = (uint8_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_uint8(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_UINT16: {
                uint16_t *data = (uint16_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_uint16(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_UINT32: {
                uint32_t *data = (uint32_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_uint32(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_UINT64: {
                uint64_t *data = (uint64_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_uint64(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_FLOAT: {
                float *data = (float*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_float(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_DOUBLE: {
                double *data = (double*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                    } else {
                        data[row] = duckdb_value_double(result, col, row);
                    }
                }
                break;
            }
            
            case MMAP_TYPE_VARCHAR: {
                uint64_t *data = (uint64_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                        data[row] = UINT64_MAX;  // Invalid index for NULL
                    } else {
                        char *str = duckdb_value_varchar(result, col, row);
                        if (str) {
                            size_t len = strlen(str);
                            
                            // Check if we have enough space in the string table
                            if (next_string_offset + len + 1 <= mmap_result->string_data_size) {
                                // Copy the string to the string table
                                memcpy(mmap_result->string_table + next_string_offset, str, len + 1);
                                
                                // Set the string offset
                                mmap_result->string_offsets[next_string_idx] = next_string_offset;
                                
                                // Set the string index in the data array
                                data[row] = next_string_idx;
                                
                                // Update counters
                                next_string_idx++;
                                next_string_offset += len + 1;
                            }
                            
                            duckdb_free(str);
                        } else {
                            // Empty string
                            data[row] = UINT64_MAX;
                        }
                    }
                }
                break;
            }
            
            case MMAP_TYPE_BLOB: {
                uint64_t *data = (uint64_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                        data[row*2] = UINT64_MAX;  // Invalid offset for NULL
                        data[row*2+1] = 0;  // Zero size for NULL
                    } else {
                        duckdb_blob blob = duckdb_value_blob(result, col, row);
                        if (blob.data && blob.size > 0) {
                            // Check if we have enough space in the string table
                            if (next_string_offset + blob.size <= mmap_result->string_data_size) {
                                // Copy the blob data to the string table
                                memcpy(mmap_result->string_table + next_string_offset, blob.data, blob.size);
                                
                                // Set the blob offset and size in the data array
                                data[row*2] = next_string_offset;
                                data[row*2+1] = blob.size;
                                
                                // Update offset counter
                                next_string_offset += blob.size;
                            }
                            
                            duckdb_free(blob.data);
                        } else {
                            // Empty blob
                            data[row*2] = UINT64_MAX;
                            data[row*2+1] = 0;
                        }
                    }
                }
                break;
            }
            
            default: {
                // For other types, convert to string for now
                uint64_t *data = (uint64_t*)((char*)mmap_result->data_buffer + col_meta->offset);
                
                for (idx_t row = 0; row < mmap_result->row_count; row++) {
                    bool is_null = duckdb_value_is_null(result, col, row);
                    
                    if (is_null) {
                        nullmap[row / 8] |= (1 << (row % 8));
                        data[row] = UINT64_MAX;  // Invalid index for NULL
                    } else {
                        char *str = duckdb_value_varchar(result, col, row);
                        if (str) {
                            size_t len = strlen(str);
                            
                            // Check if we have enough space in the string table
                            if (next_string_offset + len + 1 <= mmap_result->string_data_size) {
                                // Copy the string to the string table
                                memcpy(mmap_result->string_table + next_string_offset, str, len + 1);
                                
                                // Set the string offset
                                mmap_result->string_offsets[next_string_idx] = next_string_offset;
                                
                                // Set the string index in the data array
                                data[row] = next_string_idx;
                                
                                // Update counters
                                next_string_idx++;
                                next_string_offset += len + 1;
                            }
                            
                            duckdb_free(str);
                        } else {
                            // Empty string
                            data[row] = UINT64_MAX;
                        }
                    }
                }
                break;
            }
        }
    }
}

// Reference counting and cleanup

void retain_mmap_result(mmap_result_t *mmap_result) {
    if (mmap_result) {
        mmap_result->ref_count++;
    }
}

void release_mmap_result(mmap_result_t *mmap_result) {
    if (mmap_result && mmap_result->ref_count > 0) {
        mmap_result->ref_count--;
        if (mmap_result->ref_count == 0) {
            free_mmap_result(mmap_result);
        }
    }
}

void free_mmap_result(mmap_result_t *mmap_result) {
    if (!mmap_result) {
        return;
    }
    
    // Free buffers
    if (mmap_result->columns) {
        free(mmap_result->columns);
    }
    
    if (mmap_result->data_buffer) {
        free(mmap_result->data_buffer);
    }
    
    if (mmap_result->string_table) {
        free(mmap_result->string_table);
    }
    
    if (mmap_result->string_offsets) {
        free(mmap_result->string_offsets);
    }
    
    // Free the result structure
    free(mmap_result);
}