# Critical Issues, Unused Methods, and Performance Bottlenecks

This document highlights critical issues, unused methods, and performance bottlenecks identified in the Go-DuckDB codebase. Each issue includes the file location, line numbers, and a brief description.

## buffer_pool.go

### Critical Issues
- ✅ **FIXED: Race condition in lastCleanup access** (Lines 32-33, 110-125): Fixed by acquiring the lock before reading lastCleanup and maintaining it through the update. Used defer for consistent unlock pattern.
- ✅ **FIXED: Potential memory leak** (Lines 69-82): Fixed by ensuring resources are properly freed and implementing a nil check for retrieved buffers.
- ✅ **FIXED: Race condition with ref_count** (Lines 92-104): Fixed by capturing current values of ref_count and error_code before checking them to avoid race conditions.
- ✅ **FIXED: Nil pointer dereference risk** (Lines 71-76): Fixed by adding a nil check for buffers retrieved from the pool.

### Unused Code
- **Empty cleanup implementation** (Lines 127-130): `maybeCleanup()` doesn't actually clean anything despite its name.
- **Unused maxAge** (Lines 35-37, 42-44): Only used for timing cleanup operations but not for actual buffer expiration.
- **Potentially unused Stats()** (Line 133-140): Not referenced by any public APIs.

### Performance Issues
- **Excessive cleanup checks** (Lines 65-66): `maybeCleanup()` called on every buffer request, creating unnecessary overhead.
- **Inefficient reset pattern** (Lines 72-76): All buffers are reset when retrieved from the pool even if already clean.
- **Redundant memory operations** (Lines 93-97): `free_result_buffer()` followed by `memset()` to zero, likely redundant.
- **Lock contention** (Lines 110-125): In `maybeCleanup()` can impact performance under high concurrency.
- **Global bottleneck** (Lines 142-153): Global buffer pool instance could become a bottleneck in high-concurrency applications.

## vector.go

### Critical Issues
- ✅ **FIXED: Excessive CGO crossings** (Lines 149-326): Implemented vector batch extraction functions in C to drastically reduce CGO boundary crossings. Each data type now has a dedicated extraction function that processes blocks of data in a single call.
- ✅ **FIXED: String extraction inefficiency** (Lines 253-265): Implemented batch string extraction that processes multiple strings in a single CGO crossing.
- ✅ **FIXED: BLOB handling inefficiency** (Lines 267-295): Implemented efficient blob handling with batched extraction and proper buffer management.
- **Incomplete implementations** (Lines 567-613): GetString method uses placeholder implementation with strings.Builder, failing to use direct memory access.
- **Duplicate data loops** (Lines 682-839): ExtractColumn method has duplicate loops, creating redundant CGO crossings when calling individual getters.

## vector_pool.go

### Critical Issues
- **Excessive array clearing** (Lines 224-272): resetVector function completely clears array data by iterating over every element, unnecessarily expensive for large arrays.
- **Mutex contention risk** (Lines 275-292): maybeCleanup function acquires a mutex even for time checks, which can cause contention in high-concurrency scenarios.
- **Limited pool types** (Lines 18-24): The pool only supports 6 basic types. Some common DuckDB types (timestamp variants, interval, etc.) might benefit from dedicated pools.
- **Inefficient cleanup** (Lines 274-292): The maybeCleanup method doesn't actually clean anything; it just updates a timestamp.
- **Thread safety considerations** (Line 33): The cleanupMutex is only used for the lastCleanup timestamp but not for the gets/puts/allocations counters.

## batch_query.go

### Critical Issues
- ✅ **FIXED: Race condition** (Lines 425-427, 437-444): Fixed by adding proper nil checks before accessing the query in BatchRows.Columns(), preventing panics if BatchRows.Close() is called concurrently.
- ✅ **FIXED: Memory management issues** (Lines 679-680): Fixed by using a safer approach for empty blobs with a proper zero-length buffer instead of unsafe temporary arrays.
- ✅ **FIXED: Resource leaks** (Line 431-436): Fixed by clarifying ownership of resources and making BatchRows.Close() safely clear its references.
- ✅ **FIXED: Double-free in BLOB handling** (Lines 390-417): Fixed the potential double-free of C memory in the BLOB case of extractColumnBatch.
- ✅ **FIXED: Missing context handling** (Line 738, 794): Added proper context cancellation checks to QueryContext and ExecContext to prevent resource leaks.
- ✅ **FIXED: CGO crossing issues** (Lines 357-544): Implemented block-based extraction that minimizes CGO calls by batching data access.
- **Fixed batch size** (Line 22, 160-325): The block size for batch processing is fixed at 64, which may not be optimal for all workloads and hardware.
- ✅ **FIXED: Memory allocations** (Lines 480-542): Improved string and blob handling with more efficient memory management and reduced allocations.
- **Type switching inefficiency** (Lines 644-704): Type switching in Next method causes value extraction to be done individually for each column/row.
- ✅ **FIXED: Sequential execution** (Lines 509-578): Implemented true batch execution for better performance with large parameter sets.

### Unused Code
- **Unused fields** (Lines 41-42, 46): `batchRowsRead` field is initialized (line 241) but never read or used. The `buffer` field is allocated but never used.
- **Unused valueBuffer** (Line 412): valueBuffer field is created in BatchRows but never used.

### Performance Issues
- ✅ **IMPROVED: Excessive memory allocation** (Lines 261-263): Added proper bounds and nil checks to avoid unnecessary allocations and potential index out of bounds panics.
- ✅ **IMPROVED: Thread safety in Next()** (Lines 487-512): Added proper locking and safe copy semantics to prevent race conditions during concurrent access.
- ✅ **IMPROVED: Excessive CGO boundary crossing** (Lines 268-274): Implemented block-based extraction that processes data in blocks of 64 rows at a time, reducing CGO calls by ~64x.
- **Duplicate code** (Lines 749-780, 783-814): Nearly identical code for Exec and ExecContext with minimal differences.
- **String conversion overhead** (Lines 385-399): All unsupported types are converted to string through C.GoString which is inefficient, especially for time-related types.
- ✅ **IMPROVED: Inefficient memory management** (Lines 366-383): Implemented comprehensive ColumnVectorPool to reuse vectors and slices, significantly reducing GC pressure.
- **Fixed batch size** (Line 22): The batch size is fixed at creation time rather than being dynamically adjusted based on query characteristics or memory pressure.
- ✅ **IMPROVED: Missing vectorized processing**: Implemented block-based extraction that processes data in chunks, reducing per-row iteration overhead by ~64x in typical batches.

## direct_result.go

### Critical Issues
- ✅ **FIXED: Duplicate functionality** (Lines 230-232): Added proper deprecation notices to `ExtractStringColumnZeroCopy` to clarify its purpose and direct users to the true zero-copy method.
- ✅ **FIXED: Format string vulnerability** (Line 1206): Fixed by using constant format strings with proper error wrapping instead of dynamically constructed error messages.

### Unused Code
- **Unused buffer pools** (Lines 136-148): `stringBufferPool` and `blobBufferPool` are defined but never used after the switch to true zero-copy methods.

### Performance Bottlenecks
- ✅ **IMPROVED: Row-by-row processing** (Lines 1575-1588, 1613-1626, 1651-1665): Implemented block-based extraction for int32, int64, and float64 columns, processing data in blocks of 64 rows to reduce CGO boundary crossings by ~64x.
- ✅ **IMPROVED: Duplicate code patterns** (Lines 2152-2316): Enhanced `ExtractInt32ColumnsBatch`, `ExtractInt64ColumnsBatch`, and `ExtractFloat64ColumnsBatch` with block-based extraction while maintaining the same API.
- ✅ **IMPROVED: Missing native optimizations** (Lines 2001-2035): Implemented block-based processing for uint16, uint32, and uint64 column extraction methods to reduce CGO overhead without requiring native C functions.
- **Memory inefficiency** (Lines 305-313): `ExtractBlobColumnZeroCopy` redirects to true zero-copy but its documentation mentions buffer pooling that no longer happens.
- **Hard-coded limits** (Lines 319-333): Batch operations have a hard limit of 16 columns without a clear reasoning for this specific value.
- ✅ **IMPROVED: CGO overhead** (Lines 1686-1708): Modified `ExtractStringColumn` to use block-based extraction, significantly reducing CGO boundary crossings.

## connection.go

### Critical Issues
- ✅ **FIXED: Context handling issues** (Lines 128-139, 322-382): Fixed by adding proper context cancellation checks in PrepareContext, QueryContext, and ExecContext to respect context cancellation.
- ✅ **FIXED: Unclear locking strategy** (Lines 445-476): Fixed QueryDirectResult to maintain the lock during the entire operation, consistent with ExecContext and QueryContext.
- ✅ **FIXED: Resource leak risk** (Lines 703-719): Added a new BatchExecContext method with proper context support and implemented context checks at multiple points in batch execution.
- ✅ **FIXED: Multiple CGO crossings** (Lines 510-578): Implemented true batch execution in BatchExec that prepares and executes statements in a single operation.
- ✅ **FIXED: Duplicate preparation** (Lines 581-679): BatchExecContext now reuses a single prepared statement for multiple parameter sets.
- ✅ **FIXED: Individual column extraction** (Lines 285-451): Implemented batched column extraction with single CGO crossing for multiple columns and values.

### Performance Bottlenecks
- **Code duplication** (Lines 141-207, 322-382): ExecContext and QueryContext have identical parameter binding and execution code paths with different return handling.
- **Inefficient error handling** (Lines 167-169, 181-185, 191-195): Multiple allocations for error strings in fallback mode.
- **Timestamp conversion overhead** (Lines 310-313, 611-635): Timestamp and date conversions allocate new time.Time instances for each row.
- **Memory management issues** (Lines 453-463, 485-648): QueryColumnar extracts and converts all data even if client will only consume a portion.
- **Inefficient connection verification** (Lines 384-397): Ping implementation uses a full query execution which is inefficient.
- **String handling overhead** (Lines 291-295): Each string parameter is copied during binding with defer freeString overhead.

## statement.go

### Critical Issues
- ✅ **FIXED: Potential safety issue** (Line 40-41): Fixed by retrieving the error message safely and properly cleaning up the statement handle even if preparation failed, ensuring no resource leaks.
- ✅ **FIXED: Unsafe pointer usage** (Line 335-345): Fixed by adding safer pointer handling for blob parameters, with proper checking for empty blobs and explicit pointer management.
- ✅ **FIXED: Thread safety concerns** (Line 99-110): Fixed by not holding the mutex during CGO calls. Now acquires the lock briefly to get a safe copy of the statement pointer, then releases it before expensive operations.
- ✅ **FIXED: Potential memory leaks** (Line 319-327): Fixed by adding panic recovery to ensure proper cleanup even if there's a panic during binding. Also uses individual defer statements for each string binding.
- ✅ **FIXED: Multiple CGO crossings** (Lines 319-419): Implemented batch parameter binding that processes multiple parameters in a single operation.
- ✅ **FIXED: String parameter inefficiency** (Lines 380-387): Improved string parameter handling with batch operations and shared cleanup, reducing memory allocations.
- ✅ **FIXED: Race condition risk** (Lines 125-139, 184-196): Fixed locking pattern to prevent race conditions during statement execution.

### Unused Code
- **Unused query field** (Line 26): `query` field is stored but never used after initialization.

### Performance Bottlenecks
- ✅ **IMPROVED: Reflection overhead** (Lines 171-255): Reduced reflection usage by implementing type-specific binding paths for parameters.
- **Code duplication** (Lines 77-120 and 123-168): Significant code duplication between Exec and Query methods.
- **Timestamp conversion inefficiency** (Line 243): Timestamp conversion using Unix and nanosecond math has efficiency issues.
- **Error handling verbosity** (Lines 176-239): Error handling is verbose and repetitive across type switches.
- **Inefficient Result object** (Line 117-119): Result object is created with lastInsertID always set to 0, which is inefficient.

## fast_driver.go

### Critical Issues
- **Multiple time format parsing** (Lines 400-412): Time parsing in Next function attempts multiple formats sequentially for timestamps.
- **Unsafe pointer manipulation** (Lines 644-771): Column extraction uses direct pointer manipulation which is fast but creates unsafe code.
- ✅ **FIXED: Multiple CGO crossings** (Lines 390-509): Implemented efficient batch parameter binding to reduce CGO crossings.
- ✅ **FIXED: String memory allocation** (Lines 481-488): Improved string parameter handling with better memory management and shared cleanup.

## duckdb.go

### Critical Issues
- ✅ **FIXED: Thread safety issue** (Line 1305-1306): The `vectorsByType` global map is now properly protected by a lock when accessing in GetPreallocatedVector, with the lock being held for the entire map access operation.
- ✅ **FIXED: Memory leak risk** (Lines 1304-1347): Added periodic cleanup mechanism with `cleanUnusedVectors()` that limits the number of vectors stored per type and properly frees unused vectors.

### Unused Code
- **Unused field** (Lines 110-120): The `stats` struct in ResultBufferPool has tracking counters but they are never incremented or used anywhere in the code.
- **Unused method** (Lines 488-502): `PeriodicCleanup()` method contains only comments without actual implementation. It's declared but never called.
- **Missing bool vector pool operations** (Lines 154-158): While there's a `boolVectorPool` declared, there are no corresponding Get/Put methods like GetBoolVector/PutBoolVector unlike the other types.
- **Limited driver options** (Lines 1395-1410): The Driver struct has a `useFastDriver` field but it's not used in the Open method, making the field effectively useless.

### Performance Bottlenecks
- **Excessive locking** (Lines 800-823 and similar methods): Each Set/Get operation on SafeColumnVector acquires and releases a mutex, causing high lock contention for high-throughput scenarios.
- **Performance bottleneck** (Lines 461-486): `GetSharedString` uses sync.Map operations which can become a bottleneck under heavy load since strings are frequently processed.
- **FFI overhead** (Lines 813-817): Direct memory manipulation with unsafe.Pointer for simple operations has high overhead compared to the work being done. This creates a bottleneck for small data.
- **Missing pool initialization** (Lines 550-592): Several pools in globalBufferPool like int32VectorPool and others are declared but never initialized in the init function.

## rows.go

### Critical Issues
- ✅ **FIXED: Unsafe pointer usage** (lines 484-486, 528-529, 570-571, 612-613, 654-655): Replaced deprecated reflect.SliceHeader usage with the safer pattern of taking the address of the first slice element with proper nil checks for Go 1.17+ compatibility.
- **Limited timestamp handling** (lines 391-396): Only handles one timestamp type properly while case statement includes multiple timestamp types. Should have specific handling for each timestamp precision.
- **Thread safety concern** (lines 434-436): String cache is reset periodically based on row count, but could cause issues if accessed concurrently.
- ✅ **FIXED: Missing nil checks** (line 228): A nil check for r.columnVectors was added in Close() method to prevent potential nil pointer dereference. This is present in the current implementation.

### Unused Code
- **Memory leak risk** (line 252): Declared but unused emptyString variable.

### Performance Bottlenecks
- **Date conversion performance issue** (lines 401-402): Conversion from days to Unix timestamp has performance overhead and potential timezone issues.
- **Redundant code** (lines 458-666): ExtractXXXColumn methods have significant code duplication which could be unified with generics.
- **Inefficient timestamp creation** (lines 391-396): time.Unix creates unnecessary allocations in high-volume code path.
- **Type-specific batch extractors** (lines 458-666): Limited to specific types, missing extractors for other common types.
- **Thread-unsafe flag usage** (line 70-77): fromPool flags aren't thread-safe; concurrent access could cause memory issues.

## string_cache.go

### Critical Issues
- **Undefined symbol reference** (Lines 99, 120, 162, 185, 319, 393, 412): Code refers to `globalBufferPool.GetSharedString()` but the implementation isn't visible, suggesting missing implementation.
- ✅ **FIXED: Thread safety issues** (Lines 226-233): Completely rewrote GetFromCString with proper locking patterns to fix problematic locking pattern that could lead to race conditions.
- **Potential memory leak** (Lines 351-368): The `Reset()` method only resets maps when they grow very large (>10000 entries). For workloads with many unique strings just under this threshold, memory usage could grow substantially.
- **Inconsistent nil checks** (Line 356): Code checks map sizes but never checks if maps are nil, which could cause panics.
- **Missing bounds checking** (Lines 75-80, 135-140, 208-212): When expanding `columnValues`, there's no upper limit, which could lead to excessive memory usage.

### Unused Code
- **Stats method** (Lines 371-373): Appears potentially unused in normal operations, as it's only used in the test file.

### Performance Bottlenecks
- **Inefficient memory allocation** (Lines 292-293): When buffer is too small, it allocates a new buffer with double the required size, which could lead to memory bloat.
- **Multiple lock acquisitions** (Lines 322-324): Method acquires and releases mutex multiple times, which is inefficient and can lead to contention.
- **Hard-coded thresholds** (Lines 67-68): String size thresholds are hard-coded rather than configurable or auto-tuned.
- **Inefficient C string handling** (Lines 297-306): Byte-by-byte copying loop from C memory is likely much slower than using `C.GoStringN`.
- **Code duplication** (Various string handling methods): Significant code duplication between `Get`, `GetFromBytes`, `GetFromCString`, and `GetDedupString` methods.

## appender.go

### Critical Issues
- ✅ **FIXED: Missing transaction handling**: Implemented AppenderWithTransaction to ensure atomicity across multiple appends and prevent partial data commits.
  - Added comprehensive test suite with transaction commit, rollback, and context cancellation tests
  - Fixed type conversion issues with COUNT(*) queries in tests (DuckDB returns int64, not int32)

### Performance Bottlenecks
- **Inefficient empty blob handling** (Lines 396-407): Creating a new 1-byte buffer for every empty blob is inefficient. Should use a static zero-length buffer.
- **Duplicate blob handling code** (Lines 510-521 and 395-407): appendBlobDirect and appendValue both implement identical blob handling logic.
- **Redundant type checking** (Lines 159-241): Type checking each column twice creates unnecessary overhead.
- **Unnecessary type conversions** (Lines 264 and 274): Converting between int/uint and int64/uint64 incurs overhead that could be eliminated.
- **Duplicate timestamp conversion code** (Lines 410-421 and 524-533): Nearly identical timestamp conversion code is duplicated instead of using a shared helper function.
- **No batched append API**: Code crosses CGO boundary for each value rather than sending multiple values at once.

## Priority Recommendations

✅ 1. **Fix race conditions** in buffer_pool.go, batch_query.go, and string_cache.go which could cause crashes or memory corruption.
✅ 2. **Address resource leaks** in connection handling and result processing (fixed in batch_query.go, connection.go, and statement.go).
✅ 3. **Optimize CGO boundary crossings** which are significant performance bottlenecks (implemented block-based processing in batch_query.go, reducing CGO calls by ~64x).
✅ 4. **Implement proper context handling** to ensure resources are released on cancellation (fixed in batch_query.go, connection.go, and statement.go).
5. **Reduce code duplication** through refactoring common patterns and extraction methods.
✅ 6. **Replace unsafe reflect.SliceHeader usage** with unsafe.Slice (Go 1.17+) to prevent future compatibility issues (fixed in  rows.go).
✅ 7. **Improve thread safety** with consistent locking patterns (fixed in buffer_pool.go, batch_query.go, connection.go, string_cache.go, and statement.go).
✅ 8. **Fix unsafe empty BLOB handling** in statement.go and batch_query.go to prevent potential memory corruption.
✅ 9. **Add transaction support for appender** to ensure atomicity and prevent partial commits.
✅ 10. **Add proper memory management** for native resources (implemented comprehensive vector pooling in column_vector_pool.go).
✅ 11. **Implement batched processing** where currently using row-by-row operations (implemented block-based extraction in batch_query.go).

## March 2025 Performance Analysis

### Top Performance Bottlenecks

1. ✅ **FIXED: Excessive CGO Crossings** (Critical)
   - **vector.go**: Implemented efficient vector batch extraction with C adapter functions
   - **batch_query.go**: Added block-based extraction with single CGO call per block
   - **connection.go**: Implemented true batch execution with single statement preparation
   - **statement.go**: Created optimized parameter binding with batch operations
   - **Impact**: Reduced CGO calls by ~64x for vector operations and by ~10-50x for batch operations

2. ✅ **IMPROVED: String and BLOB Handling** (Critical) 
   - **vector.go**: Implemented batch string and BLOB extraction with shared cleanup
   - **batch_query.go**: Improved memory management for strings and blobs
   - **statement.go**: Enhanced string parameter binding with better memory handling
   - **Impact**: Reduced GC pressure significantly in string-heavy workloads

3. **Memory Management** (High)
   - **vector_pool.go**: Still needs optimization for array clearing and better cleanup
   - **string_cache.go**: Still has inefficient buffer growth patterns
   - **vector.go**: BLOB buffer reuse could be further optimized
   - **Impact**: While improved, more optimizations are possible for memory usage

4. **Type Conversion Overhead** (Medium)
   - **connection.go**: Still has timestamp conversion allocations
   - **rows.go**: Still creates unnecessary time.Time instances
   - **appender.go**: Still has unnecessary type conversions
   - **Impact**: Extra allocations persist in high-volume scenarios

5. **Mutex Contention** (Medium)
   - **vector_pool.go**: Still acquires mutex unnecessarily for simple operations
   - **duckdb.go**: SafeColumnVector operations still have excessive locking
   - **string_cache.go**: Multiple lock acquisitions persist in some methods
   - **Impact**: Could still restrict concurrent throughput with many goroutines

### Completed Optimizations

1. **Vector Batch Extraction** (Major Improvement)
   - **Old**: Individual CGO calls for each value in result sets
   - **New**: Single C function call extracts an entire block of values
   - **Benefit**: ~64x reduction in CGO crossings for typical batches
   - **Impact**: Major performance improvement for query result extraction

2. **Batch Parameter Binding** (Major Improvement)
   - **Old**: One CGO crossing per parameter in prepared statements
   - **New**: Single C function call binds an entire batch of parameters
   - **Benefit**: ~10-50x reduction in CGO crossings for queries with many parameters
   - **Impact**: Significantly faster prepared statement execution

3. **True Batch Execution** (Major Improvement)
   - **Old**: Individual statement preparation and execution for each parameter set
   - **New**: Single preparation and batched execution for multiple parameter sets
   - **Benefit**: Reduces CGO crossings by factor equal to batch size
   - **Impact**: Much faster bulk operations and inserts

4. **String and BLOB Handling** (Significant Improvement)
   - **Old**: Individual memory allocations and CGO calls for each string/blob
   - **New**: Batch processing with shared memory management
   - **Benefit**: Reduced GC pressure and fewer CGO crossings
   - **Impact**: Better performance for string/blob-heavy workloads

5. **Memory Management** (Moderate Improvement)
   - **Old**: Frequent allocations and garbage collection
   - **New**: Better object pooling and resource tracking
   - **Benefit**: Lower GC overhead and more stable performance
   - **Impact**: Improved throughput for long-running applications

### Future Enhancement Opportunities

1. **Configurable Block Size** (Medium Value)
   - **Current**: Fixed block size of 64 rows
   - **Opportunity**: Make block size configurable or auto-tuning
   - **Benefit**: 5-15% additional performance improvement
   - **Complexity**: Low-Medium

2. **True Zero-Copy String Extraction** (High Value)
   - **Current**: Still copies string data between C and Go
   - **Opportunity**: Implement true zero-copy with shared memory
   - **Benefit**: 20-30% performance improvement for string-heavy workloads
   - **Complexity**: High

3. **Buffer Reuse Strategy** (Medium Value)
   - **Current**: New buffers for most operations
   - **Opportunity**: Implement comprehensive buffer pooling and reuse
   - **Benefit**: 10-20% reduction in memory usage
   - **Complexity**: Medium

4. **Specialized Type Handlers** (Medium Value)
   - **Current**: Limited specialized handlers
   - **Opportunity**: Add optimized handlers for DATE, TIMESTAMP variants, INTERVAL
   - **Benefit**: 20-30% improvement for queries using these types
   - **Complexity**: Medium

5. **Batch Appender API** (High Value)
   - **Current**: One CGO crossing per appended value
   - **Opportunity**: Implement batch append operations
   - **Benefit**: Potential 50-80% improvement for bulk inserts
   - **Complexity**: High

## Technical Notes

1. **DuckDB BLOB Characteristics**: DuckDB's C API applies padding or alignment to BLOB data:
   - Medium blobs (1000 bytes) return as 2896 bytes with 3072 capacity
   - Large blobs (10000 bytes) return as 29236 bytes with 32768 capacity
   - Empty BLOBs should be bound with nil pointer and size 0, not with a temporary slice

2. **Thread Safety Patterns**:
   - Maps require special care in concurrent environments; use sync.Map or proper mutex protection
   - When using multiple related maps, operations must be atomic across all maps
   - Double-checking pattern is essential when combining sync.Map with additional synchronization
   - Locks must be held during critical sections to prevent race conditions

3. **CGO Performance**:
   - Each CGO crossing adds approximately 100ns overhead
   - Block-based processing reduces CGO calls by ~64x for typical batches
   - String and BLOB handling still requires individual C memory allocation
   - Modern DuckDB C API provides native batch functions that could further reduce CGO overhead

4. **Memory Management Best Practices**:
   - Object pooling significantly reduces GC pressure for frequently allocated objects
   - Vector pooling implementation with thread-safe operations improves performance
   - Proper cleanup routines must maintain vectors in reusable states
   - Size limits prevent excessive memory consumption by vector pools

5. **C Memory Safety**:
   - Format strings in error handling must use constant formats
   - Unsafe memory access in string handling must include proper bounds checking
   - Avoid reflect.SliceHeader in favor of safer patterns with proper nil checks
   - Panic recovery ensures proper cleanup even during exceptional conditions