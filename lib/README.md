# DuckDB Pre-compiled Libraries

This directory contains pre-compiled static libraries for DuckDB v1.2.1 for various platforms.

## Directory Structure

```
lib/
├── darwin/
│   ├── amd64/
│   │   └── libduckdb.a
│   └── arm64/
│       └── libduckdb.a
├── linux/
│   ├── amd64/
│   │   └── libduckdb.a
│   └── arm64/
│       └── libduckdb.a
└── windows/
    └── amd64/
        └── libduckdb.a
```

## Usage

The Go driver automatically uses the correct library for the current platform during compilation.

## About

These libraries are compiled from the official DuckDB source with no modifications.

## Building Libraries

To build your own DuckDB static libraries:

1. Clone the DuckDB repository:
   ```
   git clone https://github.com/duckdb/duckdb.git
   cd duckdb
   ```

2. Checkout the desired version (v1.2.1):
   ```
   git checkout v1.2.1
   ```

3. Configure with CMake for static library:
   ```
   mkdir -p build
   cd build
   cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF ..
   ```

4. Build:
   ```
   make -j8
   ```

5. Copy the built libduckdb.a to the correct directory in this project.