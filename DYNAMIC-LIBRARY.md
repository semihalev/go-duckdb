# Dynamic Library Implementation for go-duckdb

This document explains the implementation of the dynamic library approach for go-duckdb which eliminates the need for CGO in the driver.

## Overview

The traditional approach using CGO has several limitations:
1. Requires CGO to be enabled at build time
2. Complicates cross-compilation
3. Makes it harder to distribute as a standard Go module
4. Tightly couples the Go code with C code

The new dynamic library approach addresses these issues by:
1. Separating the native optimization code into standalone dynamic libraries
2. Using pure Go FFI (Foreign Function Interface) to load and call functions from these libraries
3. Providing a pure Go fallback implementation when the native libraries aren't available
4. Supporting cross-platform operation without requiring C compilers

## Implementation Details

### Architecture

The implementation consists of:

1. **Dynamic Library Loader**: Platform-specific code to load native libraries at runtime
   - Windows: Uses `syscall.LoadLibrary` and related functions
   - Unix: Uses `github.com/ebitengine/purego` for dynamic loading

2. **Symbol Resolution**: Code to fetch function pointers from the loaded library
   - Implemented with careful error handling and fallbacks

3. **Function Wrappers**: Type-safe Go wrappers around function pointers
   - Handles parameter marshaling and result unmarshaling
   - Checks for availability before attempting calls

4. **Pure Go Fallbacks**: Alternative implementations that work without native libraries
   - Provides consistent behavior across all platforms
   - May be slower but ensures functionality

5. **Library Distribution**: Pre-built libraries for all major platforms
   - `lib/darwin/arm64/libduckdbnative.dylib`
   - `lib/darwin/amd64/libduckdbnative.dylib`
   - `lib/linux/amd64/libduckdbnative.so`
   - `lib/linux/arm64/libduckdbnative.so`
   - `lib/windows/amd64/duckdbnative.dll`
   - `lib/windows/arm64/duckdbnative.dll`

### Key Files

- `native.go`: Core loading logic and function pointers
- `native_unix.go`: Unix-specific library loading using purego
- `native_windows.go`: Windows-specific library loading using syscall
- `native_fallback.go`: Pure Go fallback implementations
- `native_api.go`: Public API for native optimization information
- `src/CMakeLists.txt`: Build configuration for the native libraries
- `src/build.sh`: Script to build libraries for multiple platforms
- `src/duckdb_native.c`: SIMD-optimized native implementations

### Building the Native Libraries

The native libraries can be built using the provided build system:

```bash
# Build for the current platform only
make native-current

# Build for all supported platforms (requires appropriate toolchains)
make native-dynamic
```

The build process automatically detects the CPU architecture and enables appropriate SIMD optimizations (AVX2 for x86_64, NEON for ARM64).

## Using the Driver

From a user's perspective, the usage remains the same:

```go
import (
    "database/sql"
    _ "github.com/semihalev/go-duckdb"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    // ...
}
```

The driver will automatically:
1. Try to load the appropriate native library for the platform
2. Fall back to pure Go implementation if the library isn't available
3. Use native optimizations when available for better performance

## Checking Native Optimization Status

Users can check if native optimizations are available:

```go
import "github.com/semihalev/go-duckdb"

// Get brief version information
versionInfo := duckdb.GetVersionInfo()
fmt.Println(versionInfo)

// Get detailed native optimization information
nativeInfo := duckdb.GetNativeOptimizationInfo()
fmt.Println(nativeInfo)
```

## Performance Considerations

1. **With Native Optimizations**: Full SIMD acceleration, zero-copy data extraction
2. **Without Native Optimizations**: Still functional, but without SIMD and with more memory copies

The pure Go fallback ensures the driver works everywhere, but with the native libraries, it achieves near-native performance for many operations.

## Distribution

This approach simplifies distribution:
1. The Go code is pure Go, making it compatible with all Go tooling
2. The pre-built libraries can be included in the repository or downloaded separately
3. Cross-compilation works without special setup
4. Users without a C compiler can still use the driver

## Future Improvements

1. Runtime CPU feature detection for more targeted SIMD optimizations
2. Automated library download if not found locally
3. Additional platform support
4. More comprehensive pure Go fallback implementations