package duckdb

import (
	"fmt"
	"runtime"
)

// NativeInfo returns information about the native library status
type NativeInfo struct {
	Available    bool   // Whether native optimizations are available
	Architecture string // Current architecture (arm64, amd64, etc.)
	Platform     string // Current platform (darwin, linux, windows)
	Path         string // Path to the loaded native library, if available
	Error        string // Error message if native optimizations failed to load
	SIMDFeatures string // Available SIMD features (AVX2, NEON, etc.)
}

// GetNativeOptimizationInfo returns detailed information about the native optimization status
func GetNativeOptimizationInfo() NativeInfo {
	// This will trigger loading if it hasn't happened yet
	loadNativeLibrary()

	info := NativeInfo{
		Available:    nativeLibLoaded,
		Architecture: runtime.GOARCH,
		Platform:     runtime.GOOS,
		Path:         nativeLibPath,
	}

	if nativeLibError != nil {
		info.Error = nativeLibError.Error()
	}

	// Determine SIMD features based on architecture
	switch runtime.GOARCH {
	case "amd64":
		if hasAVX2() {
			info.SIMDFeatures = "AVX2, FMA"
		} else {
			info.SIMDFeatures = "SSE2"
		}
	case "arm64":
		info.SIMDFeatures = "NEON"
	default:
		info.SIMDFeatures = "None"
	}

	return info
}

// String returns a human-readable summary of the native optimization status
func (i NativeInfo) String() string {
	if i.Available {
		return fmt.Sprintf("Native optimizations: Available\nPlatform: %s/%s\nSIMD: %s\nLibrary: %s",
			i.Platform, i.Architecture, i.SIMDFeatures, i.Path)
	}

	return fmt.Sprintf("Native optimizations: Not available\nPlatform: %s/%s\nError: %s",
		i.Platform, i.Architecture, i.Error)
}

// Simple CPU feature detection for x86_64
// In a real implementation, this would use runtime CPU feature detection
func hasAVX2() bool {
	// For now, assume AVX2 is available on amd64
	// In a real implementation, use CPUID or equivalent
	return runtime.GOARCH == "amd64"
}

// VersionInfo returns information about the DuckDB driver
type VersionInfo struct {
	DriverVersion   string // Version of the Go driver
	DuckDBVersion   string // Version of the embedded DuckDB library
	GoVersion       string // Go runtime version
	NativeAvailable bool   // Whether native optimizations are available
}

// This variable is left empty since GetDuckDBVersion is implemented in version.go

// GetVersionInfo returns version information about the driver and DuckDB
func GetVersionInfo() VersionInfo {
	return VersionInfo{
		DriverVersion:   "0.2.0", // Update this with your driver version
		DuckDBVersion:   GetDuckDBVersion(),
		GoVersion:       runtime.Version(),
		NativeAvailable: NativeOptimizationsAvailable(),
	}
}

// String returns a human-readable summary of version information
func (v VersionInfo) String() string {
	nativeStr := "Not available"
	if v.NativeAvailable {
		nativeStr = "Available"
	}

	return fmt.Sprintf("Driver version: %s\nDuckDB version: %s\nGo version: %s\nNative optimizations: %s",
		v.DriverVersion, v.DuckDBVersion, v.GoVersion, nativeStr)
}
