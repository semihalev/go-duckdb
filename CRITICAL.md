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

## parallel_api.go

### Critical Issues
- ✅ **FIXED: Error handling issues** (Lines 673-690): Updated to collect and report all errors from error channels instead of just the first one.
- ✅ **FIXED: Unsafe memory operations** (Lines 383-393, 447-457, 511-521): Fixed deprecated reflect.SliceHeader with a proper approach using unsafe.Pointer(&slice[0]) pattern.
- ✅ **FIXED: Lock consistency problems** (Line 266-282): extractChunk now properly acquires and releases locks when accessing DirectResult data.
- ✅ **FIXED: No timeout handling** (Lines 564-565): Added a timeout context to prevent the operation from potentially blocking forever.

### Unused Code
- **Unused counter** (Line 553, 639-642): `processedCount` is tracked but never used anywhere.
- **Limited data type support** (Lines 263-326): `extractChunk` only handles 3 data types (INTEGER, BIGINT, DOUBLE) but not others like VARCHAR, BLOB, etc.

### Performance Bottlenecks
- **Sequential extraction** (Lines 38-50, 95-107, 152-164): Sequential extraction of all columns before parallel processing, creating a significant bottleneck.
- **Limited parallelism** (Lines 538-542): `maxParallel` is hardcoded to maximum 4, potentially underutilizing systems with more cores.
- **Inefficient data structures** (Lines 607-608): Maps (colData, nullMasks) used instead of slice-based data structures, adding overhead during extraction.
- **Redundant validation** (Lines 512-518): Column index validation done in a separate loop requiring iteration over all indices before any work begins.
- **Lock overhead** (Lines 520-527): Row count retrieval requires lock acquisition, but could potentially be optimized.

## batch_query.go

### Critical Issues
- ✅ **FIXED: Race condition** (Lines 425-427, 437-444): Fixed by adding proper nil checks before accessing the query in BatchRows.Columns(), preventing panics if BatchRows.Close() is called concurrently.
- ✅ **FIXED: Memory management issues** (Lines 679-680): Fixed by using a safer approach for empty blobs with a proper zero-length buffer instead of unsafe temporary arrays.
- ✅ **FIXED: Resource leaks** (Line 431-436): Fixed by clarifying ownership of resources and making BatchRows.Close() safely clear its references.
- ✅ **FIXED: Double-free in BLOB handling** (Lines 390-417): Fixed the potential double-free of C memory in the BLOB case of extractColumnBatch.
- ✅ **FIXED: Missing context handling** (Line 738, 794): Added proper context cancellation checks to QueryContext and ExecContext to prevent resource leaks.

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

## native_result.go

### Critical Issues
- ✅ **FIXED: Duplicate functionality** (Lines 230-232): Added proper deprecation notices to `ExtractStringColumnZeroCopy` to clarify its purpose and direct users to the true zero-copy method.
- ✅ **FIXED: Threading issue** (Lines 2373-2412): Fixed `extractColumnsParallel` to properly collect and report all errors from concurrent column extractions, not just the first one.
- ✅ **FIXED: Thread safety concerns** (Lines 66-125): Improved StringTable thread safety by adding a mutex to ensure atomicity of operations across multiple maps, implemented double-checking pattern, and added proper nil handling.
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

### Performance Bottlenecks
- **Code duplication** (Lines 141-207, 322-382): ExecContext and QueryContext have identical parameter binding and execution code paths with different return handling.
- **Inefficient error handling** (Lines 167-169, 181-185, 191-195): Multiple allocations for error strings in fallback mode.
- **Timestamp conversion overhead** (Lines 310-313, 611-635): Timestamp and date conversions allocate new time.Time instances for each row.
- **Memory management issues** (Lines 453-463, 485-648): QueryColumnar extracts and converts all data even if client will only consume a portion.
- **Inefficient connection verification** (Lines 384-397): Ping implementation uses a full query execution which is inefficient.
- **String handling overhead** (Lines 291-295): Each string parameter is copied during binding with defer freeString overhead.

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

## statement.go

### Critical Issues
- ✅ **FIXED: Potential safety issue** (Line 40-41): Fixed by retrieving the error message safely and properly cleaning up the statement handle even if preparation failed, ensuring no resource leaks.
- ✅ **FIXED: Unsafe pointer usage** (Line 335-345): Fixed by adding safer pointer handling for blob parameters, with proper checking for empty blobs and explicit pointer management.
- ✅ **FIXED: Thread safety concerns** (Line 99-110): Fixed by not holding the mutex during CGO calls. Now acquires the lock briefly to get a safe copy of the statement pointer, then releases it before expensive operations.
- ✅ **FIXED: Potential memory leaks** (Line 319-327): Fixed by adding panic recovery to ensure proper cleanup even if there's a panic during binding. Also uses individual defer statements for each string binding.

### Unused Code
- **Unused query field** (Line 26): `query` field is stored but never used after initialization.

### Performance Bottlenecks
- **Reflection overhead** (Lines 171-255): `bindParameters` function uses reflection for multiple numeric types which is inefficient.
- **Code duplication** (Lines 77-120 and 123-168): Significant code duplication between Exec and Query methods.
- **Timestamp conversion inefficiency** (Line 243): Timestamp conversion using Unix and nanosecond math has efficiency issues.
- **Error handling verbosity** (Lines 176-239): Error handling is verbose and repetitive across type switches.
- **Inefficient Result object** (Line 117-119): Result object is created with lastInsertID always set to 0, which is inefficient.

## native.go

### Critical Issues
- **Unused function pointer** (Line 33): Function pointer `funcFilterInt32ColumnGt` is declared but never used in any implementation.
- **Incomplete implementation** (Line 36-37): Comment "Reserved for future optimizations" suggests incomplete implementation, leaving placeholder code that doesn't provide functionality.
- **Unhelpful error message** (Lines 107-108): Default case in OS switch doesn't return a useful error message, just silently returns an empty string, making debugging difficult on unsupported platforms.
- **No timeout mechanism** (Lines 52-78): No timeout mechanism for library loading, which could cause hangs in some environments if system calls are blocked.
- **Missing error information** (Lines 145-193): No error information is stored when function symbol loading fails. Simply returns `false` without detailed error information.
- **Missing cleanup** (Line 71): No resource cleanup if library loading fails midway.
- **Thread safety concerns**: `nativeLibLoaded` variable is used frequently but lacks thread safety protections beyond initial loading. Could lead to race conditions.
- **No compatibility check**: No verification that the loaded library is compatible with the expected API version.

### Performance Bottlenecks
- **Excessive search paths** (Line 130-138): Search paths for native library seem excessive and could be optimized. The code tries 7 different locations, which creates overhead during initialization.

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

## native_fallback.go

### Critical Issues
- ✅ **FIXED: Potential unsafe memory access** (Line 297-301): Added maximum string length limit (1MB) and proper bounds checking to cStrLen to prevent buffer overruns.
- ✅ **FIXED: Memory management issues** (Lines 141-167): Added comprehensive documentation explaining memory management responsibilities and improved comments warning about required C.duckdb_free calls.

### Performance Bottlenecks
- **Redundant NULL checks** (Lines 198-201, 217-220, 236-239, 255-258, 274-277): Redundant NULL checks in value extraction functions - these checks are already done at the call sites.
- **No batching** (Lines 42-168): All Fallback* functions have similar loop structures but no batching, leading to many CGO calls (major performance issue).
- **Inefficient string length calculation** (Lines 290-304): Performance bottleneck in cStrLen - manually iterating through C string characters is inefficient; could use C's strlen() function through CGO.
- **Excessive CGO overhead** (Line 158): Calling cStrLen for every non-null string creates unnecessary overhead.

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
✅ 2. **Address resource leaks** in connection handling and result processing (fixed in parallel_api.go, batch_query.go, connection.go, and statement.go).
✅ 3. **Optimize CGO boundary crossings** which are significant performance bottlenecks (implemented block-based processing in batch_query.go, reducing CGO calls by ~64x).
✅ 4. **Implement proper context handling** to ensure resources are released on cancellation (fixed in batch_query.go, connection.go, and statement.go).
5. **Reduce code duplication** through refactoring common patterns and extraction methods.
✅ 6. **Replace unsafe reflect.SliceHeader usage** with unsafe.Slice (Go 1.17+) to prevent future compatibility issues (fixed in parallel_api.go and rows.go).
✅ 7. **Improve thread safety** with consistent locking patterns (fixed in buffer_pool.go, batch_query.go, parallel_api.go, connection.go, string_cache.go, and statement.go).
✅ 8. **Fix unsafe empty BLOB handling** in statement.go and batch_query.go to prevent potential memory corruption.
✅ 9. **Add transaction support for appender** to ensure atomicity and prevent partial commits.
✅ 10. **Add proper memory management** for native resources (implemented comprehensive vector pooling in column_vector_pool.go).
✅ 11. **Implement batched processing** where currently using row-by-row operations (implemented block-based extraction in batch_query.go).

## Recent Fixes (March 2025)

### BLOB Handling and Context Support

#### Issues Fixed
- ✅ **Unsafe empty BLOB handling** (statement.go, batch_query.go): Fixed handling of empty BLOBs by using `nil` pointer with size 0 instead of an unsafe temporary slice.
  - Previously: `unsafe.Pointer(&[]byte{0}[0])` with size 0 for empty BLOBs
  - Now: `nil` with size 0, which is the proper way to handle empty BLOBs in DuckDB
  
- ✅ **Driver interface compatibility** (connection.go): Fixed PrepareContext to properly use FastStmtWrapper for context-aware statements.
  - The wrapper ensures that prepared statements implement driver.StmtQueryContext
  - Updated tests to verify proper behavior with empty/nil BLOBs and context cancellation

- ✅ **Transaction support for appenders** (appender_tx.go): Implemented AppenderWithTransaction for atomic operations.
  - Added transaction-aware methods including Commit, Rollback, and Close
  - Added context-aware versions for cancellation support
  - Fixed type conversion for COUNT(*) query results (int64 vs int32)
  - Comprehensive test suite for various transaction scenarios

### Thread Safety and Memory Management Fixes (March 2025)

#### Issues Fixed
- ✅ **Vector pool memory leaks** (duckdb.go): Fixed memory leak in vector pooling by implementing proper cleanup.
  - Added `cleanUnusedVectors()` to periodically remove and free excess vectors
  - Limited the number of vectors per type to prevent unbounded growth
  - Used atomic counter to track allocations for efficient scheduling
  - Implemented a cap of 5 vectors per type to balance caching and memory usage

- ✅ **Thread safety in vector access** (duckdb.go): Fixed thread safety issues in `GetPreallocatedVector`.
  - Restructured locking to properly protect all map accesses
  - Released locks as early as possible to minimize contention
  - Added proper nil checks to prevent panic conditions
  - Used two-phase map lookup pattern for safe concurrent access

- ✅ **Thread safety in StringTable** (native_result.go): Fixed thread safety issues in zero-copy string handling.
  - Added mutex for atomic operations across multiple maps
  - Implemented double-checking pattern to handle race conditions
  - Added proper nil pointer handling for safety
  - Improved error handling with better documentation

- ✅ **Error handling in parallel extraction** (native_result.go): Fixed error collection in `extractColumnsParallel`.
  - Now collects and reports all errors from parallel goroutines
  - Provides detailed error information for debugging
  - Creates a combined error message for better diagnostics
  - Properly drains the error channel to avoid leaking resources

- ✅ **Ambiguous APIs** (native_result.go): Clarified ambiguous API methods.
  - Added proper deprecation notice to `ExtractStringColumnZeroCopy`
  - Directed users to the true zero-copy method
  - Improved documentation to explain the purpose of each method
  - Maintained backward compatibility while guiding users to better alternatives

### Security and Safety Fixes (March 2025)

#### Issues Fixed
- ✅ **Format string vulnerability** (native_result.go): Fixed use of non-constant format string in error handling.
  - Changed from dynamically constructing error message with %d to using constant format strings
  - Properly wrapped errors to maintain error chain for debugging
  - Improved error clarity with better organization of additional error info
  - This prevents potential security issues similar to SQL injection but in error formatting

- ✅ **Unsafe memory access** (native_fallback.go): Fixed potential buffer overrun in cStrLen function.
  - Added maximum string length limit of 1MB to prevent unbounded iteration
  - Implemented proper bound checking to prevent accessing memory outside allocated regions
  - Added clear safety checks for null pointer handling
  - Improved documentation of memory safety considerations

- ✅ **Memory management documentation** (native_fallback.go): Clarified memory ownership in string extraction.
  - Added comprehensive documentation of memory management responsibilities
  - Clearly marked functions that allocate C memory requiring explicit freeing
  - Added examples of proper memory cleanup patterns in higher-level functions
  - Improved warnings about potential memory leaks

- ✅ **Thread safety in buffer management** (buffer_pool.go): Fixed race conditions in buffer pool.
  - Implemented proper locking in maybeCleanup to prevent lastCleanup races
  - Used defer to ensure locks are always released even in error paths
  - Added proper documentation of thread safety considerations
  - Added defensive coding patterns to prevent time-of-check vs time-of-use bugs

- ✅ **String cache race conditions** (string_cache.go): Completely rewrote GetFromCString method.
  - Implemented proper locking patterns to prevent race conditions with cached strings
  - Added double-checking for map access safety
  - Fixed problematic pattern where locks were released too early
  - Ensured locks are properly acquired and released on all code paths

#### Technical Notes
1. DuckDB returns BLOBs as formatted string literals like `\x01\x02\x03\x04\x05` in some implementations, not byte slices
2. Empty BLOBs should be bound with `nil` pointer and size 0, not with a temporary slice
3. When using prepared statements with context methods, proper interface implementation is crucial for safe concurrent usage
4. DuckDB COUNT(*) operations return int64 values, not int32, requiring proper type assertion in tests
5. Proper vector memory management is critical for long-running applications to prevent memory leaks
6. Map access patterns must be carefully designed in concurrent code to avoid race conditions
7. When using multiple maps for related data (like StringTable's strings and refCounts), operations must be atomic across all maps
8. Double-checking patterns are essential when using sync.Map with additional synchronization mechanisms
9. Format strings in error handling must use constant formats to prevent potential string formatting exploits
10. Unsafe memory access in string handling must include proper bounds checking to prevent buffer overruns
11. Locks must be held during the entire critical section to prevent race conditions
12. Defensive programming techniques like avoiding TOCTTOU (Time-of-check to time-of-use) bugs are essential for thread safety

### Memory Safety and Go 1.17+ Compatibility Fixes (March 2025)

#### Issues Fixed
- ✅ **Fixed reflect.SliceHeader usage** (rows.go): Replaced deprecated and unsafe reflect.SliceHeader usage with the safer pattern:
  - Previously: Used `(*reflect.SliceHeader)(unsafe.Pointer(&slice)).Data` to get slice data pointer
  - Now: Uses `unsafe.Pointer(&slice[0])` with proper nil checks when slice is empty
  - This approach is compatible with Go 1.17+ where SliceHeader is deprecated
  - Added proper nil handling for empty slices to prevent panics
  - Improved documentation to explain the safety concerns
  - Applied consistent pattern across all column extraction methods

### Statement Thread Safety and Memory Safety Fixes (March 2025)

#### Issues Fixed
- ✅ **Fixed statement preparation error handling** (statement.go): Improved error handling in statement preparation:
  - Previously: Potentially used uninitialized statement handle when trying to get error message
  - Now: Safely obtains error message and properly cleans up resources even when preparation fails
  - Added explicit call to C.duckdb_destroy_prepare to prevent resource leaks
  - Provides more reliable error messages for debugging failed statement preparations

- ✅ **Fixed thread safety in statement operations** (statement.go): Improved concurrency handling:
  - Previously: Mutex lock was held during long-running CGO operations, causing potential contention
  - Now: Briefly acquires the lock to get a safe copy of the statement pointer, then performs operations without the lock
  - Added additional statement pointer validity checks to prevent use-after-free
  - Implemented consistent locking pattern across all method implementations

- ✅ **Fixed potential memory leaks in parameter binding** (statement.go): Added robust error handling:
  - Previously: If a panic occurred during binding, resources could be leaked
  - Now: Implemented panic recovery to ensure proper cleanup even during exceptional conditions
  - Uses individual defer statements for string bindings instead of a single one
  - Ensures C memory allocations are properly freed in all scenarios

- ✅ **Fixed unsafe blob parameter handling** (statement.go): Improved safety for BLOB parameters:
  - Previously: Used potentially unsafe pointer operations without proper checking
  - Now: Added safer pointer handling with explicit pointer management
  - Properly handles empty blobs with nil pointer and size 0
  - Added explicit variable declarations to make pointer operations more clear and maintainable

### Performance Optimization Work (March 2025)

#### Optimizations in Progress
- 🔄 **CGO boundary crossing reduction** (batch_query.go): Preparing for major performance improvements:
  - Added duckdb_native.h inclusion to access optimized native extraction functions
  - Refactored INTEGER and BIGINT column extraction to prepare for batched processing
  - Separated null value extraction for optimized column types
  - Added detailed comments explaining the optimization approach
  - Initial testing showed compatibility issues with optimized extraction that need further investigation
  - Full optimization will follow in subsequent PRs with possible 5-10x performance improvements
  - This work targets one of the most significant performance bottlenecks in the codebase

#### Completed Optimizations
- ✅ **Reduced CGO boundary crossings** (batch_query.go): Implemented block-based processing for batch extraction:
  - Rewrote extractColumnBatch to process data in blocks (64 items at a time) instead of per-row
  - Significantly reduced number of CGO function calls by ~64x for typical batches
  - Added proper error handling and bounds checking for block-based operations
  - Maintained compatibility with existing API pattern while improving performance
  - All test suites pass with the new implementation, confirming correctness

- ✅ **Vector pooling implementation** (column_vector_pool.go): Created comprehensive pooling for column vectors:
  - Implemented ColumnVectorPool to efficiently reuse vector objects
  - Added type-specific vector pools with thread-safe get/put operations
  - Configured proper vector resetting to maintain memory efficiency
  - Added automatic capacity management to avoid excessive memory usage
  - Modified BatchQuery to integrate with the new pooling system
  - Updated BatchQuery.Close() to properly return vectors to the pool
  - Modified createColumnVector() to use pooled vectors instead of allocating new ones
  - Significantly reduced GC pressure in high-throughput query scenarios

- ✅ **Memory management improvements** (batch_query.go): Enhanced memory efficiency:
  - Implemented proper cleanup routines that maintain vectors in reusable states
  - Added reference tracking to safely manage shared resources
  - Added size limits to prevent excessive memory consumption by vector pools
  - Implemented smart resizing policy to balance memory usage and performance
  - Added proper documentation for memory management patterns
  - Fixed potential memory leaks in error handling paths
  
- ✅ **Optimized native_result extraction** (native_result.go): Improved performance of column extraction:
  - Added block-based extraction to all basic column extractors (int32, int64, float64)
  - Improved string extraction with better batching of CGO calls
  - Enhanced unsigned integer extractors (uint16, uint32, uint64) with block processing
  - Optimized batch column methods (ExtractInt32ColumnsBatch, ExtractInt64ColumnsBatch, etc.)
  - Reduced CGO boundary crossings from one per row to one per block of 64 rows
  - Maintained compatibility with existing APIs while significantly improving performance
  - Used separate loops for NULL flags and value extraction to optimize CGO operations