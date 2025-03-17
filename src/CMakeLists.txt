cmake_minimum_required(VERSION 3.12)
project(duckdb_native LANGUAGES C)

# Detect processor architecture
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(ARCH_FLAGS "-mavx2 -mfma")
    set(ARCH "x86_64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(ARCH_FLAGS "-march=armv8-a+simd+fp")
    set(ARCH "arm64")
    add_definitions(-D__ARM_NEON)
else()
    message(WARNING "Unknown architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    set(ARCH "unknown")
endif()

# Determine platform-specific settings
if(WIN32)
    set(PLATFORM "windows")
    set(LIB_PREFIX "")
    set(LIB_EXTENSION ".dll")
elseif(APPLE)
    set(PLATFORM "darwin")
    set(LIB_PREFIX "lib")
    set(LIB_EXTENSION ".dylib")
else()
    set(PLATFORM "linux")
    set(LIB_PREFIX "lib")
    set(LIB_EXTENSION ".so")
endif()

# Set output directories
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY 
    ${CMAKE_BINARY_DIR}/lib/${PLATFORM}/${ARCH})

# Find the DuckDB library path based on platform and architecture
if(APPLE)
    set(DUCKDB_LIB_DIR "${CMAKE_SOURCE_DIR}/../lib/darwin/${ARCH}")
elseif(WIN32)
    set(DUCKDB_LIB_DIR "${CMAKE_SOURCE_DIR}/../lib/windows/${ARCH}")
else()
    set(DUCKDB_LIB_DIR "${CMAKE_SOURCE_DIR}/../lib/linux/${ARCH}")
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/../include
)

# Add library directories for linking
link_directories(${DUCKDB_LIB_DIR})

# Common compiler flags
add_compile_options(-O3 -fPIC -Wall)
add_compile_options(${ARCH_FLAGS})

# Define source files
set(SOURCES
    duckdb_native.c
)

# Create shared library
add_library(duckdbnative SHARED ${SOURCES})
set_target_properties(duckdbnative PROPERTIES
    PREFIX "${LIB_PREFIX}"
    SUFFIX "${LIB_EXTENSION}"
)

# Link against the DuckDB library and C++ standard library
if(APPLE)
    target_link_libraries(duckdbnative duckdb "-lstdc++")
elseif(WIN32)
    target_link_libraries(duckdbnative duckdb "stdc++")
else()
    target_link_libraries(duckdbnative duckdb "stdc++")
endif()

# Print some information about the build
message(STATUS "Building for platform: ${PLATFORM}, architecture: ${ARCH}")
message(STATUS "DuckDB library directory: ${DUCKDB_LIB_DIR}")

# Install rules
install(TARGETS duckdbnative
    LIBRARY DESTINATION lib/${PLATFORM}/${ARCH}
    RUNTIME DESTINATION lib/${PLATFORM}/${ARCH}
)