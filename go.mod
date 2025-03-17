module github.com/semihalev/go-duckdb

go 1.24

// Required for dynamic library loading on Unix platforms
// This removes the need for CGO when using the driver
require github.com/ebitengine/purego v0.8.2
