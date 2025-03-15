package duckdb

/*
#include <duckdb.h>

#ifndef DUCKDB_VERSION_MAJOR
#define DUCKDB_VERSION_MAJOR 0
#endif

#ifndef DUCKDB_VERSION_MINOR
#define DUCKDB_VERSION_MINOR 0
#endif

#ifndef DUCKDB_VERSION_PATCH
#define DUCKDB_VERSION_PATCH 0
#endif

// DuckDB version string for versions that don't provide it
static const char* get_version_string() {
    static char buffer[32];
    #ifdef DUCKDB_SOURCE_ID
    return DUCKDB_SOURCE_ID;
    #else
    sprintf(buffer, "%d.%d.%d", DUCKDB_VERSION_MAJOR, DUCKDB_VERSION_MINOR, DUCKDB_VERSION_PATCH);
    return buffer;
    #endif
}
*/
import "C"
import (
	"fmt"
	"strings"
)

// Version represents the DuckDB version information
type Version struct {
	Major      int
	Minor      int
	Patch      int
	VersionStr string
}

// String returns the version as a string
func (v Version) String() string {
	if v.VersionStr != "" {
		return v.VersionStr
	}
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// AtLeast checks if the version is at least the given major, minor, patch
func (v Version) AtLeast(major, minor, patch int) bool {
	if v.Major > major {
		return true
	}
	if v.Major < major {
		return false
	}
	// Major is equal, check minor
	if v.Minor > minor {
		return true
	}
	if v.Minor < minor {
		return false
	}
	// Minor is equal, check patch
	return v.Patch >= patch
}

// IsAtLeast120 returns true if the version is at least 1.2.0
func (v Version) IsAtLeast120() bool {
	return v.AtLeast(1, 2, 0)
}

// GetDuckDBVersion returns the version of the linked DuckDB library
func GetDuckDBVersion() Version {
	versionStr := C.GoString(C.get_version_string())
	
	// Parse version from the string - handle both regular versions and git commits
	major := int(C.DUCKDB_VERSION_MAJOR)
	minor := int(C.DUCKDB_VERSION_MINOR)
	patch := int(C.DUCKDB_VERSION_PATCH)
	
	// For git commits, the version string might be something like "v0.8.0-1014-gf41c0e9a4e"
	// We'll try to extract the version number from that
	if strings.HasPrefix(versionStr, "v") {
		parts := strings.Split(versionStr[1:], "-")
		if len(parts) > 0 {
			// Parse version numbers from the version part
			versionParts := strings.Split(parts[0], ".")
			if len(versionParts) >= 3 {
				// Try to parse but don't override if parsing fails
				if v, err := fmt.Sscanf(parts[0], "%d.%d.%d", &major, &minor, &patch); err == nil && v == 3 {
					// Successfully parsed
				}
			}
		}
	}
	
	return Version{
		Major:      major,
		Minor:      minor,
		Patch:      patch,
		VersionStr: versionStr,
	}
}

// VersionString returns the DuckDB version as a string
func VersionString() string {
	return GetDuckDBVersion().String()
}

// AreArithmeticExceptionsFatal checks if arithmetic exceptions are fatal in this DuckDB version
func AreArithmeticExceptionsFatal() bool {
	// In DuckDB 1.2.0+, arithmetic exceptions are not fatal by default
	return !GetDuckDBVersion().IsAtLeast120()
}